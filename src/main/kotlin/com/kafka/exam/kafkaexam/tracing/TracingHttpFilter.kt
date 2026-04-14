package com.kafka.exam.kafkaexam.tracing

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

/**
 * HTTP 요청에 대한 root span을 시작하고, 수신한 traceparent가 있으면 그 컨텍스트를 이어받습니다.
 * 응답 헤더에도 traceparent를 돌려주어 클라이언트에서 추적 가능하게 합니다.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
class TracingHttpFilter(
    private val tracer: Tracer
) : OncePerRequestFilter() {

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val incoming = TraceContext.parse(request.getHeader(TraceContext.HEADER_TRACEPARENT))
        val prev = tracer.currentContext()

        val span = if (incoming != null) {
            tracer.startSpanFromParent(
                name = "http ${request.method} ${request.requestURI}",
                kind = SpanKind.SERVER,
                parent = incoming
            )
        } else {
            tracer.startSpan(
                name = "http ${request.method} ${request.requestURI}",
                kind = SpanKind.SERVER
            )
        }

        span.setAttribute("http.method", request.method)
        span.setAttribute("http.path", request.requestURI)
        span.setAttribute("http.remote", request.remoteAddr ?: "unknown")

        response.setHeader(TraceContext.HEADER_TRACEPARENT, tracer.currentContext()!!.toTraceparent())

        try {
            filterChain.doFilter(request, response)
            span.setAttribute("http.status", response.status)
            if (response.status >= 500) {
                span.status = "ERROR"
            }
        } catch (e: Throwable) {
            span.markError(e)
            throw e
        } finally {
            tracer.endSpan(span)
            tracer.setContext(prev)
        }
    }
}