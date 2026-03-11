package com.kafka.exam.kafkaexam.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy

/**
 * Kafka 메트릭 수집기
 *
 * Micrometer를 사용하여 다음 메트릭을 수집:
 * - Consumer Lag (파티션별)
 * - Producer 처리량 (messages/sec)
 * - Consumer 처리량 (messages/sec)
 * - 에러율
 * - 응답 시간
 */
@Component
class KafkaMetricsCollector(
    private val meterRegistry: MeterRegistry,
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private lateinit var adminClient: AdminClient

    // 메트릭 저장소
    private val consumerLagByPartition = ConcurrentHashMap<String, AtomicLong>()
    private val messagesProduced = AtomicLong(0)
    private val messagesConsumed = AtomicLong(0)
    private val messagesFailed = AtomicLong(0)
    private val producerErrors = AtomicLong(0)
    private val consumerErrors = AtomicLong(0)

    // 처리 시간 타이머
    private lateinit var producerTimer: Timer
    private lateinit var consumerTimer: Timer

    @PostConstruct
    fun init() {
        adminClient = AdminClient.create(mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
        ))

        registerMetrics()
        log.info("KafkaMetricsCollector initialized")
    }

    @PreDestroy
    fun cleanup() {
        adminClient.close()
    }

    private fun registerMetrics() {
        // Producer 메트릭
        Gauge.builder("kafka.producer.messages.total") { messagesProduced.get().toDouble() }
            .description("Total messages produced")
            .register(meterRegistry)

        Gauge.builder("kafka.producer.errors.total") { producerErrors.get().toDouble() }
            .description("Total producer errors")
            .register(meterRegistry)

        producerTimer = Timer.builder("kafka.producer.send.time")
            .description("Producer send time")
            .register(meterRegistry)

        // Consumer 메트릭
        Gauge.builder("kafka.consumer.messages.total") { messagesConsumed.get().toDouble() }
            .description("Total messages consumed")
            .register(meterRegistry)

        Gauge.builder("kafka.consumer.errors.total") { consumerErrors.get().toDouble() }
            .description("Total consumer errors")
            .register(meterRegistry)

        Gauge.builder("kafka.consumer.messages.failed") { messagesFailed.get().toDouble() }
            .description("Total failed messages")
            .register(meterRegistry)

        consumerTimer = Timer.builder("kafka.consumer.process.time")
            .description("Consumer processing time")
            .register(meterRegistry)

        // 성공률
        Gauge.builder("kafka.producer.success.rate") {
            val total = messagesProduced.get()
            val errors = producerErrors.get()
            if (total > 0) ((total - errors).toDouble() / total) * 100 else 100.0
        }
            .description("Producer success rate (%)")
            .register(meterRegistry)

        Gauge.builder("kafka.consumer.success.rate") {
            val total = messagesConsumed.get()
            val failed = messagesFailed.get()
            if (total > 0) ((total - failed).toDouble() / total) * 100 else 100.0
        }
            .description("Consumer success rate (%)")
            .register(meterRegistry)
    }

    /**
     * Consumer Lag 메트릭 등록
     */
    fun registerLagMetric(topic: String, partition: Int) {
        val key = "$topic-$partition"
        if (!consumerLagByPartition.containsKey(key)) {
            consumerLagByPartition[key] = AtomicLong(0)

            Gauge.builder("kafka.consumer.lag") { consumerLagByPartition[key]?.get()?.toDouble() ?: 0.0 }
                .description("Consumer lag per partition")
                .tags(Tags.of("topic", topic, "partition", partition.toString()))
                .register(meterRegistry)
        }
    }

    /**
     * Consumer Lag 업데이트
     */
    fun updateLag(topic: String, partition: Int, lag: Long) {
        val key = "$topic-$partition"
        registerLagMetric(topic, partition)
        consumerLagByPartition[key]?.set(lag)
    }

    // === 메트릭 기록 메서드 ===

    fun recordProducerSuccess() {
        messagesProduced.incrementAndGet()
    }

    fun recordProducerError() {
        producerErrors.incrementAndGet()
    }

    fun recordProducerTime(durationMs: Long) {
        producerTimer.record(Duration.ofMillis(durationMs))
    }

    fun recordConsumerSuccess() {
        messagesConsumed.incrementAndGet()
    }

    fun recordConsumerError() {
        consumerErrors.incrementAndGet()
    }

    fun recordConsumerFailure() {
        messagesFailed.incrementAndGet()
    }

    fun recordConsumerTime(durationMs: Long) {
        consumerTimer.record(Duration.ofMillis(durationMs))
    }

    // === Lag 수집 스케줄러 ===

    @Scheduled(fixedDelayString = "\${kafka.metrics.lag.interval:30000}")
    fun collectConsumerLag() {
        try {
            val consumerGroups = adminClient.listConsumerGroups().all().get()

            for (group in consumerGroups) {
                if (!group.groupId().startsWith(groupId)) continue

                val offsets = adminClient.listConsumerGroupOffsets(group.groupId())
                    .partitionsToOffsetAndMetadata().get()

                for ((tp, offsetAndMetadata) in offsets) {
                    val endOffsets = getEndOffsets(listOf(tp))
                    val endOffset = endOffsets[tp] ?: continue
                    val currentOffset = offsetAndMetadata.offset()
                    val lag = endOffset - currentOffset

                    updateLag(tp.topic(), tp.partition(), lag)

                    if (lag > 1000) {
                        log.warn(
                            "High consumer lag detected: group={}, topic={}, partition={}, lag={}",
                            group.groupId(), tp.topic(), tp.partition(), lag
                        )
                    }
                }
            }
        } catch (e: Exception) {
            log.error("Failed to collect consumer lag: {}", e.message)
        }
    }

    private fun getEndOffsets(partitions: List<TopicPartition>): Map<TopicPartition, Long> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$groupId-metrics-temp",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )

        KafkaConsumer<String, String>(props).use { consumer ->
            return consumer.endOffsets(partitions)
        }
    }

    // === 통계 조회 ===

    fun getMetricsSummary(): MetricsSummary {
        val totalLag = consumerLagByPartition.values.sumOf { it.get() }

        return MetricsSummary(
            messagesProduced = messagesProduced.get(),
            messagesConsumed = messagesConsumed.get(),
            messagesFailed = messagesFailed.get(),
            producerErrors = producerErrors.get(),
            consumerErrors = consumerErrors.get(),
            totalConsumerLag = totalLag,
            lagByPartition = consumerLagByPartition.mapValues { it.value.get() }
        )
    }

    fun resetMetrics() {
        messagesProduced.set(0)
        messagesConsumed.set(0)
        messagesFailed.set(0)
        producerErrors.set(0)
        consumerErrors.set(0)
        consumerLagByPartition.values.forEach { it.set(0) }
    }
}

data class MetricsSummary(
    val messagesProduced: Long,
    val messagesConsumed: Long,
    val messagesFailed: Long,
    val producerErrors: Long,
    val consumerErrors: Long,
    val totalConsumerLag: Long,
    val lagByPartition: Map<String, Long>
)