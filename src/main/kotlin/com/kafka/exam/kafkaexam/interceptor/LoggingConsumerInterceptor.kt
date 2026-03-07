package com.kafka.exam.kafkaexam.interceptor

import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class LoggingConsumerInterceptor : ConsumerInterceptor<String, String> {

    private val log = LoggerFactory.getLogger(javaClass)
    private val metrics = InterceptorMetrics.instance

    private var groupId: String = "unknown"
    private var clientId: String = "unknown"

    override fun configure(configs: MutableMap<String, *>) {
        groupId = configs["group.id"]?.toString() ?: "unknown"
        clientId = configs["client.id"]?.toString() ?: "unknown"
        log.info("ConsumerInterceptor configured: groupId={}, clientId={}", groupId, clientId)
    }

    override fun onConsume(records: ConsumerRecords<String, String>): ConsumerRecords<String, String> {
        if (records.isEmpty) {
            return records
        }

        val traceId = UUID.randomUUID().toString().substring(0, 8)
        MDC.put("traceId", traceId)
        MDC.put("groupId", groupId)

        try {
            val recordCount = records.count()
            val topicPartitions = records.partitions()

            log.info(
                "[CONSUME] Received {} records from {} partitions, groupId={}, traceId={}",
                recordCount,
                topicPartitions.size,
                groupId,
                traceId
            )

            for (tp in topicPartitions) {
                val partitionRecords = records.records(tp)
                if (partitionRecords.isNotEmpty()) {
                    val firstOffset = partitionRecords.first().offset()
                    val lastOffset = partitionRecords.last().offset()

                    metrics.recordConsumed(tp.topic(), partitionRecords.size)

                    log.debug(
                        "[CONSUME] topic={}, partition={}, records={}, offsets=[{}-{}]",
                        tp.topic(),
                        tp.partition(),
                        partitionRecords.size,
                        firstOffset,
                        lastOffset
                    )

                    // 메시지 지연 시간 계산
                    for (record in partitionRecords) {
                        val latency = System.currentTimeMillis() - record.timestamp()
                        metrics.recordLatency(tp.topic(), latency)

                        if (latency > 5000) {
                            log.warn(
                                "[CONSUME] High latency detected: topic={}, partition={}, offset={}, latency={}ms",
                                tp.topic(),
                                tp.partition(),
                                record.offset(),
                                latency
                            )
                        }
                    }
                }
            }
        } finally {
            MDC.remove("traceId")
            MDC.remove("groupId")
        }

        return records
    }

    override fun onCommit(offsets: MutableMap<TopicPartition, OffsetAndMetadata>) {
        if (offsets.isEmpty()) {
            return
        }

        log.debug("[COMMIT] Committing offsets for {} partitions", offsets.size)

        for ((tp, offsetMetadata) in offsets) {
            metrics.recordCommit(tp.topic())

            log.debug(
                "[COMMIT] topic={}, partition={}, offset={}, metadata={}",
                tp.topic(),
                tp.partition(),
                offsetMetadata.offset(),
                offsetMetadata.metadata() ?: "none"
            )
        }
    }

    override fun close() {
        log.info("ConsumerInterceptor closed: groupId={}, clientId={}", groupId, clientId)
        logFinalMetrics()
    }

    private fun logFinalMetrics() {
        val stats = metrics.getStats()
        log.info(
            "[METRICS] Final consumer stats - totalConsumed={}, totalCommits={}, avgLatencyMs={}",
            stats.totalConsumed,
            stats.totalCommits,
            stats.avgLatencyMs
        )
    }
}