package com.kafka.exam.kafkaexam.streams.controller

import com.kafka.exam.kafkaexam.streams.model.OrderEvent
import com.kafka.exam.kafkaexam.streams.model.OrderStatistics
import com.kafka.exam.kafkaexam.streams.producer.OrderEventProducer
import com.kafka.exam.kafkaexam.streams.service.OrderStatisticsService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.Instant
import java.time.temporal.ChronoUnit

@RestController
@RequestMapping("/api/streams/statistics")
class OrderStatisticsController(
    private val orderStatisticsService: OrderStatisticsService,
    private val orderEventProducer: OrderEventProducer
) {

    @GetMapping("/health")
    fun getStreamsHealth(): ResponseEntity<Map<String, String>> {
        val state = orderStatisticsService.getStreamsState()
        return ResponseEntity.ok(mapOf("state" to state))
    }

    @GetMapping("/products/{productId}")
    fun getProductStatistics(@PathVariable productId: String): ResponseEntity<OrderStatistics> {
        val statistics = orderStatisticsService.getStatisticsByProductId(productId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(statistics)
    }

    @GetMapping("/products")
    fun getAllStatistics(): ResponseEntity<List<OrderStatistics>> {
        val statistics = orderStatisticsService.getAllStatistics()
        return ResponseEntity.ok(statistics)
    }

    @GetMapping("/products/top")
    fun getTopProducts(@RequestParam(defaultValue = "10") limit: Int): ResponseEntity<List<OrderStatistics>> {
        val topProducts = orderStatisticsService.getTopProducts(limit)
        return ResponseEntity.ok(topProducts)
    }

    @GetMapping("/products/{productId}/hourly")
    fun getHourlyOrderCount(
        @PathVariable productId: String,
        @RequestParam(required = false) hoursAgo: Long?
    ): ResponseEntity<Map<String, Any>> {
        val hours = hoursAgo ?: 24
        val toTime = Instant.now()
        val fromTime = toTime.minus(hours, ChronoUnit.HOURS)

        val hourlyData = orderStatisticsService.getHourlyOrderCount(productId, fromTime, toTime)
        val formattedData = hourlyData.map { (timestamp, count) ->
            mapOf("timestamp" to timestamp.toString(), "orderCount" to count)
        }

        return ResponseEntity.ok(
            mapOf(
                "productId" to productId,
                "fromTime" to fromTime.toString(),
                "toTime" to toTime.toString(),
                "data" to formattedData
            )
        )
    }

    @PostMapping("/events")
    fun publishOrderEvent(@RequestBody event: OrderEvent): ResponseEntity<Map<String, String>> {
        orderEventProducer.publishOrderEvent(event)
        return ResponseEntity.ok(mapOf("status" to "published", "orderId" to event.orderId))
    }

    @PostMapping("/events/sample")
    fun publishSampleEvents(@RequestParam(defaultValue = "10") count: Int): ResponseEntity<Map<String, Any>> {
        orderEventProducer.publishSampleEvents(count)
        return ResponseEntity.ok(mapOf("status" to "published", "count" to count))
    }
}