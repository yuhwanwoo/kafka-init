package com.kafka.exam.kafkaexam.interceptor

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/interceptor/metrics")
class InterceptorMetricsController {

    private val metrics = InterceptorMetrics.instance

    @GetMapping
    fun getStats(): ResponseEntity<InterceptorStats> {
        return ResponseEntity.ok(metrics.getStats())
    }

    @GetMapping("/topics/{topic}")
    fun getTopicStats(@PathVariable topic: String): ResponseEntity<TopicStats> {
        return ResponseEntity.ok(metrics.getTopicStats(topic))
    }

    @DeleteMapping
    fun resetMetrics(): ResponseEntity<Map<String, String>> {
        metrics.reset()
        return ResponseEntity.ok(mapOf("message" to "Metrics reset successfully"))
    }
}