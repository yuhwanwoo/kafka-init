package com.kafka.exam.kafkaexam.tracing

import java.time.Instant

enum class SpanKind {
    SERVER,     // HTTP 요청 등 수신
    CLIENT,     // HTTP 호출 등 송신
    PRODUCER,   // Kafka send
    CONSUMER,   // Kafka consume
    INTERNAL    // 내부 처리
}

data class Span(
    val traceId: String,
    val spanId: String,
    val parentSpanId: String?,
    val name: String,
    val kind: SpanKind,
    val startedAt: Instant,
    var endedAt: Instant? = null,
    var durationMs: Long? = null,
    val attributes: MutableMap<String, String> = mutableMapOf(),
    var status: String = "OK",
    var errorMessage: String? = null
) {
    fun end() {
        endedAt = Instant.now()
        durationMs = endedAt!!.toEpochMilli() - startedAt.toEpochMilli()
    }

    fun markError(e: Throwable) {
        status = "ERROR"
        errorMessage = "${e.javaClass.simpleName}: ${e.message}"
    }

    fun setAttribute(key: String, value: Any?) {
        attributes[key] = value?.toString() ?: "null"
    }
}