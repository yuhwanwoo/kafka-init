package com.kafka.exam.kafkaexam.reactive

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverRecord
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

@Component
class ReactiveKafkaConsumer(
    private val kafkaReceiver: KafkaReceiver<String, String>
) {

    private val log = LoggerFactory.getLogger(ReactiveKafkaConsumer::class.java)

    private var consumerDisposable: Disposable? = null

    // 메시지 브로드캐스트를 위한 Sink (멀티 구독자 지원)
    private val messageSink = Sinks.many().multicast().onBackpressureBuffer<ReactiveMessage>()

    // 메트릭
    private val processedCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)
    private val processingTimes = ConcurrentHashMap<String, Long>()

    @PostConstruct
    fun startConsuming() {
        consumerDisposable = kafkaReceiver.receive()
            .flatMap({ record -> processRecord(record) }, 16) // 동시 처리 수
            .subscribe(
                { /* 성공 처리 완료 */ },
                { error -> log.error("Consumer error: {}", error.message, error) }
            )

        log.info("Reactive Kafka Consumer started")
    }

    @PreDestroy
    fun stopConsuming() {
        consumerDisposable?.dispose()
        log.info("Reactive Kafka Consumer stopped")
    }

    /**
     * 메시지 처리 (백프레셔 적용)
     */
    private fun processRecord(record: ReceiverRecord<String, String>): Mono<Void> {
        val startTime = System.currentTimeMillis()

        return Mono.fromCallable {
            ReactiveMessage(
                key = record.key(),
                value = record.value(),
                topic = record.topic(),
                partition = record.partition(),
                offset = record.offset(),
                timestamp = record.timestamp(),
                headers = record.headers().associate { it.key() to String(it.value()) }
            )
        }
            .doOnNext { message ->
                log.debug(
                    "Processing message: topic={}, partition={}, offset={}, key={}",
                    message.topic, message.partition, message.offset, message.key
                )

                // 구독자들에게 브로드캐스트
                messageSink.tryEmitNext(message)
            }
            .doOnSuccess {
                val processingTime = System.currentTimeMillis() - startTime
                processedCount.incrementAndGet()
                processingTimes[record.key() ?: "null"] = processingTime

                log.info(
                    "Message processed: offset={}, processingTime={}ms",
                    record.offset(), processingTime
                )

                // 오프셋 커밋
                record.receiverOffset().acknowledge()
            }
            .doOnError { error ->
                errorCount.incrementAndGet()
                log.error(
                    "Failed to process message: offset={}, error={}",
                    record.offset(), error.message
                )
            }
            .onErrorResume { error ->
                // 에러 발생해도 오프셋 커밋하고 다음 메시지 처리
                record.receiverOffset().acknowledge()
                Mono.empty()
            }
            .then()
    }

    /**
     * 메시지 스트림 구독 (SSE, WebSocket 등에서 사용)
     */
    fun messageStream(): Flux<ReactiveMessage> {
        return messageSink.asFlux()
    }

    /**
     * 필터링된 메시지 스트림
     */
    fun messageStream(filter: (ReactiveMessage) -> Boolean): Flux<ReactiveMessage> {
        return messageSink.asFlux().filter(filter)
    }

    /**
     * 특정 키의 메시지만 구독
     */
    fun messageStreamByKey(key: String): Flux<ReactiveMessage> {
        return messageStream { it.key == key }
    }

    /**
     * 윈도우 기반 배치 수신 (시간 또는 개수 기준)
     */
    fun batchedMessageStream(
        maxSize: Int = 100,
        maxTime: Duration = Duration.ofSeconds(5)
    ): Flux<List<ReactiveMessage>> {
        return messageSink.asFlux()
            .bufferTimeout(maxSize, maxTime)
    }

    /**
     * 샘플링된 메시지 스트림 (부하 조절)
     */
    fun sampledMessageStream(sampleDuration: Duration): Flux<ReactiveMessage> {
        return messageSink.asFlux()
            .sample(sampleDuration)
    }

    /**
     * 메트릭 조회
     */
    fun getMetrics(): ConsumerMetrics {
        return ConsumerMetrics(
            processedCount = processedCount.get(),
            errorCount = errorCount.get(),
            averageProcessingTimeMs = if (processingTimes.isNotEmpty()) {
                processingTimes.values.average()
            } else 0.0,
            recentProcessingTimes = processingTimes.toMap()
        )
    }

    /**
     * 메트릭 리셋
     */
    fun resetMetrics() {
        processedCount.set(0)
        errorCount.set(0)
        processingTimes.clear()
    }
}

data class ReactiveMessage(
    val key: String?,
    val value: String,
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val headers: Map<String, String> = emptyMap()
)

data class ConsumerMetrics(
    val processedCount: Long,
    val errorCount: Long,
    val averageProcessingTimeMs: Double,
    val recentProcessingTimes: Map<String, Long>
)