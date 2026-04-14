package com.kafka.exam.kafkaexam.tracing

import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.time.Instant

/**
 * Span 생성 및 현재 thread의 trace context 관리.
 *
 * ThreadLocal + MDC를 활용해 현재 활성 span을 추적하며,
 * Kafka 리스너에서 메시지 수신 시 새 context로 교체됩니다.
 */
@Component
class Tracer(
    private val spanRecorder: SpanRecorder
) {

    companion object {
        const val MDC_TRACE_ID = "traceId"
        const val MDC_SPAN_ID = "spanId"
        const val MDC_PARENT_SPAN_ID = "parentSpanId"

        private val currentContext = ThreadLocal<TraceContext?>()
    }

    fun currentContext(): TraceContext? = currentContext.get()

    /**
     * 새 context를 설정하고 이전 context를 반환합니다. 나중에 restore에 사용.
     */
    fun setContext(ctx: TraceContext?): TraceContext? {
        val prev = currentContext.get()
        if (ctx == null) {
            currentContext.remove()
            MDC.remove(MDC_TRACE_ID)
            MDC.remove(MDC_SPAN_ID)
            MDC.remove(MDC_PARENT_SPAN_ID)
        } else {
            currentContext.set(ctx)
            MDC.put(MDC_TRACE_ID, ctx.traceId)
            MDC.put(MDC_SPAN_ID, ctx.spanId)
            ctx.parentSpanId?.let { MDC.put(MDC_PARENT_SPAN_ID, it) }
                ?: MDC.remove(MDC_PARENT_SPAN_ID)
        }
        return prev
    }

    /**
     * Span 시작. 기존 context가 있으면 자식 span을 만들고, 없으면 root span 생성.
     */
    fun startSpan(name: String, kind: SpanKind): Span {
        val parentCtx = currentContext.get()
        val ctx = parentCtx?.newChild() ?: TraceContext.newRoot()
        setContext(ctx)

        return Span(
            traceId = ctx.traceId,
            spanId = ctx.spanId,
            parentSpanId = ctx.parentSpanId,
            name = name,
            kind = kind,
            startedAt = Instant.now()
        )
    }

    /**
     * 주어진 부모 context 아래에서 새 span 시작 (Kafka 메시지 수신 시 사용)
     */
    fun startSpanFromParent(name: String, kind: SpanKind, parent: TraceContext): Span {
        val childCtx = parent.newChild()
        setContext(childCtx)

        return Span(
            traceId = childCtx.traceId,
            spanId = childCtx.spanId,
            parentSpanId = childCtx.parentSpanId,
            name = name,
            kind = kind,
            startedAt = Instant.now()
        )
    }

    fun endSpan(span: Span) {
        span.end()
        spanRecorder.record(span)
    }

    fun <T> withSpan(name: String, kind: SpanKind, block: (Span) -> T): T {
        val prev = currentContext()
        val span = startSpan(name, kind)
        return try {
            block(span)
        } catch (e: Throwable) {
            span.markError(e)
            throw e
        } finally {
            endSpan(span)
            setContext(prev)
        }
    }
}