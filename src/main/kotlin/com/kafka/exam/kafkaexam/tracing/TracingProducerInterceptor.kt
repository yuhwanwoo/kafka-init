package com.kafka.exam.kafkaexam.tracing

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import org.slf4j.MDC

/**
 * Kafka Producer용 ProducerInterceptor:
 * - 현재 thread MDC의 traceId/spanId를 기반으로 traceparent 헤더를 주입
 * - 활성 context가 없으면 root context를 생성하여 주입
 *
 * Spring Kafka는 ProducerInterceptor에 직접 Bean을 주입할 수 없으므로
 * MDC를 이용해 애플리케이션 trace와 연결합니다.
 */
class TracingProducerInterceptor : ProducerInterceptor<String, String> {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun configure(configs: MutableMap<String, *>) {}

    override fun onSend(record: ProducerRecord<String, String>): ProducerRecord<String, String> {
        // 이미 헤더가 있으면 그대로 사용(상류에서 설정)
        if (record.headers().lastHeader(TraceContext.HEADER_TRACEPARENT) != null) {
            return record
        }

        val traceId = MDC.get(Tracer.MDC_TRACE_ID)
        val spanId = MDC.get(Tracer.MDC_SPAN_ID)

        val traceparent = if (traceId != null && spanId != null) {
            "00-$traceId-$spanId-01"
        } else {
            // context가 없는 background thread 등에서의 호출
            TraceContext.newRoot().toTraceparent()
        }

        record.headers().add(TraceContext.HEADER_TRACEPARENT, traceparent.toByteArray())

        log.debug(
            "[TRACE-INJECT] topic={}, key={}, traceparent={}",
            record.topic(), record.key(), traceparent
        )
        return record
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {}

    override fun close() {}
}