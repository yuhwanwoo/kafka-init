package com.kafka.exam.kafkaexam.tracing

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

/**
 * 완료된 Span을 메모리에 기록합니다. 실제 운영에서는 OTLP Exporter로
 * Jaeger/Tempo/Zipkin 등으로 전송해야 하지만, 여기서는 조회용 버퍼로만 사용.
 */
@Component
class SpanRecorder(
    private val maxSpans: Int = 1000
) {
    private val log = LoggerFactory.getLogger(SpanRecorder::class.java)

    private val spans = ConcurrentLinkedDeque<Span>()
    private val tracesIndex = ConcurrentHashMap<String, MutableList<Span>>()
    private val size = AtomicInteger(0)

    fun record(span: Span) {
        spans.addLast(span)
        tracesIndex.computeIfAbsent(span.traceId) { mutableListOf() }.add(span)

        if (size.incrementAndGet() > maxSpans) {
            val removed = spans.pollFirst()
            if (removed != null) {
                size.decrementAndGet()
                tracesIndex[removed.traceId]?.remove(removed)
                if (tracesIndex[removed.traceId]?.isEmpty() == true) {
                    tracesIndex.remove(removed.traceId)
                }
            }
        }

        log.debug(
            "[TRACE] recorded span: traceId={}, spanId={}, name={}, kind={}, durationMs={}, status={}",
            span.traceId, span.spanId, span.name, span.kind, span.durationMs, span.status
        )
    }

    fun getTrace(traceId: String): List<Span> {
        return tracesIndex[traceId]?.sortedBy { it.startedAt } ?: emptyList()
    }

    fun listTraces(limit: Int = 50): List<TraceSummary> {
        return tracesIndex.entries
            .sortedByDescending { entry -> entry.value.maxOfOrNull { it.startedAt } }
            .take(limit)
            .map { (traceId, spans) ->
                TraceSummary(
                    traceId = traceId,
                    spanCount = spans.size,
                    rootName = spans.firstOrNull { it.parentSpanId == null }?.name
                        ?: spans.first().name,
                    startedAt = spans.minOf { it.startedAt },
                    totalDurationMs = spans.mapNotNull { it.durationMs }.maxOrNull() ?: 0,
                    hasError = spans.any { it.status == "ERROR" }
                )
            }
    }

    fun clear() {
        spans.clear()
        tracesIndex.clear()
        size.set(0)
    }
}

data class TraceSummary(
    val traceId: String,
    val spanCount: Int,
    val rootName: String,
    val startedAt: java.time.Instant,
    val totalDurationMs: Long,
    val hasError: Boolean
)