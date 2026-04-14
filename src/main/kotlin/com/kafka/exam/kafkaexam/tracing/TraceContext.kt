package com.kafka.exam.kafkaexam.tracing

import java.security.SecureRandom

/**
 * W3C Trace Context (https://www.w3.org/TR/trace-context/)
 *
 * traceparent 헤더 형식: "00-{trace-id}-{parent-id}-{flags}"
 *   - version: 8 bits (2 hex chars) - "00"
 *   - trace-id: 128 bits (32 hex chars)
 *   - parent-id (span-id): 64 bits (16 hex chars)
 *   - flags: 8 bits (2 hex chars) - "01"(sampled) or "00"
 */
data class TraceContext(
    val traceId: String,
    val spanId: String,
    val parentSpanId: String? = null,
    val sampled: Boolean = true
) {

    fun toTraceparent(): String {
        val flag = if (sampled) "01" else "00"
        return "00-$traceId-$spanId-$flag"
    }

    /**
     * 자식 span 생성: traceId는 유지, 새 spanId 발급, parent는 현재 spanId
     */
    fun newChild(): TraceContext {
        return TraceContext(
            traceId = traceId,
            spanId = randomSpanId(),
            parentSpanId = spanId,
            sampled = sampled
        )
    }

    companion object {
        private val random = SecureRandom()
        private const val TRACEPARENT_HEADER = "traceparent"

        const val HEADER_TRACEPARENT = TRACEPARENT_HEADER

        fun randomTraceId(): String {
            val bytes = ByteArray(16)
            random.nextBytes(bytes)
            return bytes.toHex()
        }

        fun randomSpanId(): String {
            val bytes = ByteArray(8)
            random.nextBytes(bytes)
            return bytes.toHex()
        }

        fun newRoot(): TraceContext {
            return TraceContext(traceId = randomTraceId(), spanId = randomSpanId())
        }

        /**
         * traceparent 헤더 파싱. 형식 올바르지 않으면 null 반환
         */
        fun parse(traceparent: String?): TraceContext? {
            if (traceparent.isNullOrBlank()) return null
            val parts = traceparent.split("-")
            if (parts.size != 4) return null
            val (version, traceId, spanId, flags) = parts
            if (version != "00" || traceId.length != 32 || spanId.length != 16 || flags.length != 2) {
                return null
            }
            if (traceId.all { it == '0' } || spanId.all { it == '0' }) return null
            return TraceContext(
                traceId = traceId,
                spanId = spanId,
                sampled = flags.toInt(16) and 0x01 == 0x01
            )
        }

        private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
    }
}