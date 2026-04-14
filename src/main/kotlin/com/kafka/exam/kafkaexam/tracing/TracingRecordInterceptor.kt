package com.kafka.exam.kafkaexam.tracing

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.RecordInterceptor
import org.springframework.stereotype.Component

/**
 * Spring Kafka RecordInterceptor: 리스너 호출 전/후에 실행됩니다.
 *
 * - intercept(): 레코드의 traceparent 헤더에서 부모 context를 추출하고
 *   CONSUMER span을 시작. MDC/ThreadLocal에 context 설정
 * - success/failure(): span 종료하며 Tracer에 기록
 *
 * 리스너 메서드 안에서 KafkaTemplate으로 produce할 경우 TracingProducerInterceptor가
 * 현재 MDC를 읽어 traceparent를 주입 -> end-to-end 전파
 */
@Component
class TracingRecordInterceptor(
    private val tracer: Tracer
) : RecordInterceptor<String, String> {

    private val log = LoggerFactory.getLogger(javaClass)

    private val activeSpan = ThreadLocal<Pair<Span, TraceContext?>?>()

    override fun intercept(
        record: ConsumerRecord<String, String>,
        consumer: Consumer<String, String>
    ): ConsumerRecord<String, String> {
        val parent = extractTraceContext(record)
        val prevContext = tracer.currentContext()

        val span = if (parent != null) {
            tracer.startSpanFromParent(
                name = "kafka.consume ${record.topic()}",
                kind = SpanKind.CONSUMER,
                parent = parent
            )
        } else {
            tracer.startSpan(
                name = "kafka.consume ${record.topic()}",
                kind = SpanKind.CONSUMER
            )
        }

        span.setAttribute("messaging.system", "kafka")
        span.setAttribute("messaging.destination", record.topic())
        span.setAttribute("messaging.kafka.partition", record.partition())
        span.setAttribute("messaging.kafka.offset", record.offset())
        span.setAttribute("messaging.kafka.key", record.key() ?: "null")

        activeSpan.set(span to prevContext)

        log.debug(
            "[TRACE-EXTRACT] topic={}, partition={}, offset={}, traceId={}, spanId={}, parent={}",
            record.topic(), record.partition(), record.offset(),
            span.traceId, span.spanId, span.parentSpanId
        )
        return record
    }

    override fun success(record: ConsumerRecord<String, String>, consumer: Consumer<String, String>) {
        finishSpan(null)
    }

    override fun failure(
        record: ConsumerRecord<String, String>,
        exception: Exception,
        consumer: Consumer<String, String>
    ) {
        finishSpan(exception)
    }

    private fun finishSpan(error: Throwable?) {
        val entry = activeSpan.get() ?: return
        activeSpan.remove()
        val (span, prev) = entry
        try {
            if (error != null) span.markError(error)
            tracer.endSpan(span)
        } finally {
            tracer.setContext(prev)
        }
    }

    private fun extractTraceContext(record: ConsumerRecord<String, String>): TraceContext? {
        val header = record.headers().lastHeader(TraceContext.HEADER_TRACEPARENT) ?: return null
        return TraceContext.parse(String(header.value()))
    }
}