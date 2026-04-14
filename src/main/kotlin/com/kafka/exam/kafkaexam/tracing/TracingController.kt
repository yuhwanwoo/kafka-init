package com.kafka.exam.kafkaexam.tracing

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/tracing")
class TracingController(
    private val tracer: Tracer,
    private val spanRecorder: SpanRecorder,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    /**
     * 추적 데모: HTTP 수신 -> 내부 span -> Kafka produce -> Consumer가 동일 traceId로 이어받음
     */
    @PostMapping("/demo")
    fun demo(@RequestBody request: TracingDemoRequest): Map<String, Any?> {
        val currentCtx = tracer.currentContext()

        tracer.withSpan("business.process-order", SpanKind.INTERNAL) { span ->
            span.setAttribute("order.id", request.orderId)
            span.setAttribute("order.amount", request.amount)

            // Kafka 전송 (TracingProducerInterceptor가 현재 MDC를 읽어 traceparent 헤더 주입)
            tracer.withSpan("kafka.send tracing-demo-topic", SpanKind.PRODUCER) { producerSpan ->
                producerSpan.setAttribute("messaging.destination", "tracing-demo-topic")
                kafkaTemplate.send("tracing-demo-topic", request.orderId, request.toJson())
            }
        }

        return mapOf(
            "traceId" to currentCtx?.traceId,
            "spanId" to currentCtx?.spanId,
            "traceparent" to currentCtx?.toTraceparent(),
            "description" to "traceId로 /tracing/traces/{traceId} 조회하면 전체 trace chain 확인 가능"
        )
    }

    @GetMapping("/traces")
    fun listTraces(@RequestParam(defaultValue = "50") limit: Int): List<TraceSummary> {
        return spanRecorder.listTraces(limit)
    }

    @GetMapping("/traces/{traceId}")
    fun getTrace(@PathVariable traceId: String): Map<String, Any?> {
        val spans = spanRecorder.getTrace(traceId)
        return mapOf(
            "traceId" to traceId,
            "spanCount" to spans.size,
            "spans" to spans.map { span ->
                mapOf(
                    "spanId" to span.spanId,
                    "parentSpanId" to span.parentSpanId,
                    "name" to span.name,
                    "kind" to span.kind.name,
                    "startedAt" to span.startedAt,
                    "durationMs" to span.durationMs,
                    "status" to span.status,
                    "error" to span.errorMessage,
                    "attributes" to span.attributes
                )
            }
        )
    }

    @DeleteMapping("/traces")
    fun clear(): Map<String, String> {
        spanRecorder.clear()
        return mapOf("status" to "cleared")
    }
}

data class TracingDemoRequest(
    val orderId: String,
    val amount: Long
) {
    fun toJson(): String = """{"orderId":"$orderId","amount":$amount}"""
}