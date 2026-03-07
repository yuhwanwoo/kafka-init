package com.kafka.exam.kafkaexam.interceptor

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.UUID

class LoggingProducerInterceptor : ProducerInterceptor<String, String> {

    private val log = LoggerFactory.getLogger(javaClass)
    private val metrics = InterceptorMetrics.instance
    private val pendingRecords = mutableMapOf<String, Long>()

    private var clientId: String = "unknown"

    override fun configure(configs: MutableMap<String, *>) {
        clientId = configs["client.id"]?.toString() ?: "unknown"
        log.info("ProducerInterceptor configured: clientId={}", clientId)
    }

    override fun onSend(record: ProducerRecord<String, String>): ProducerRecord<String, String> {
        val traceId = UUID.randomUUID().toString().substring(0, 8)
        val sendTime = System.currentTimeMillis()

        MDC.put("traceId", traceId)

        try {
            // 트레이싱을 위한 헤더 추가
            val headers = record.headers()
            headers.add("X-Trace-Id", traceId.toByteArray())
            headers.add("X-Send-Time", sendTime.toString().toByteArray())
            headers.add("X-Client-Id", clientId.toByteArray())

            val recordKey = "${record.topic()}-${record.partition() ?: "null"}-$traceId"
            pendingRecords[recordKey] = sendTime

            metrics.recordSent(record.topic())

            log.info(
                "[SEND] Sending message: topic={}, key={}, partition={}, traceId={}",
                record.topic(),
                record.key() ?: "null",
                record.partition() ?: "auto",
                traceId
            )

            log.debug(
                "[SEND] Message details: valueSize={} bytes, headers={}",
                record.value()?.length ?: 0,
                headers.map { it.key() }
            )

            return record

        } finally {
            MDC.remove("traceId")
        }
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (metadata != null) {
            val topic = metadata.topic()
            val partition = metadata.partition()
            val offset = metadata.offset()

            metrics.recordAcknowledged(topic)

            log.info(
                "[ACK] Message acknowledged: topic={}, partition={}, offset={}, timestamp={}",
                topic,
                partition,
                offset,
                metadata.timestamp()
            )

            if (metadata.hasOffset()) {
                log.debug(
                    "[ACK] Serialized sizes: keySize={}, valueSize={}",
                    metadata.serializedKeySize(),
                    metadata.serializedValueSize()
                )
            }
        }

        if (exception != null) {
            metrics.recordFailed(metadata?.topic() ?: "unknown")

            log.error(
                "[ACK] Message send failed: topic={}, error={}",
                metadata?.topic() ?: "unknown",
                exception.message,
                exception
            )
        }
    }

    override fun close() {
        log.info("ProducerInterceptor closed: clientId={}", clientId)
        logFinalMetrics()
    }

    private fun logFinalMetrics() {
        val stats = metrics.getStats()
        log.info(
            "[METRICS] Final producer stats - totalSent={}, totalAcked={}, totalFailed={}, successRate={}%",
            stats.totalSent,
            stats.totalAcknowledged,
            stats.totalFailed,
            if (stats.totalSent > 0) (stats.totalAcknowledged * 100 / stats.totalSent) else 0
        )
    }
}