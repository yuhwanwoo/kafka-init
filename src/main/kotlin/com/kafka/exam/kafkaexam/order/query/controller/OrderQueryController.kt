package com.kafka.exam.kafkaexam.order.query.controller

import com.kafka.exam.kafkaexam.order.query.dto.*
import com.kafka.exam.kafkaexam.order.query.service.OrderQueryService
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime

@RestController
@RequestMapping("/api/v1/orders")
class OrderQueryController(
    private val orderQueryService: OrderQueryService
) {

    // Order Summary Endpoints

    @GetMapping("/{orderId}/summary")
    fun getOrderSummary(@PathVariable orderId: String): ResponseEntity<OrderSummaryResponse> {
        val summary = orderQueryService.getOrderSummary(orderId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(OrderSummaryResponse.from(summary))
    }

    @GetMapping("/saga/{sagaId}/summary")
    fun getOrderSummaryBySagaId(@PathVariable sagaId: String): ResponseEntity<OrderSummaryResponse> {
        val summary = orderQueryService.getOrderSummaryBySagaId(sagaId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(OrderSummaryResponse.from(summary))
    }

    // Order History Endpoints

    @GetMapping("/{orderId}/history")
    fun getOrderHistory(@PathVariable orderId: String): ResponseEntity<List<OrderHistoryResponse>> {
        val history = orderQueryService.getOrderHistory(orderId)
        return ResponseEntity.ok(history.map { OrderHistoryResponse.from(it) })
    }

    @GetMapping("/{orderId}/history/detail")
    fun getOrderHistoryDetail(@PathVariable orderId: String): ResponseEntity<List<OrderHistoryDetailResponse>> {
        val history = orderQueryService.getOrderHistory(orderId)
        return ResponseEntity.ok(history.map { OrderHistoryDetailResponse.from(it) })
    }

    // Customer Order Endpoints

    @GetMapping("/customer/{customerId}")
    fun getCustomerOrders(
        @PathVariable customerId: String,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestParam(defaultValue = "createdAt") sortBy: String,
        @RequestParam(defaultValue = "DESC") sortDir: String
    ): ResponseEntity<PagedResponse<OrderSummaryResponse>> {
        val sort = Sort.by(Sort.Direction.valueOf(sortDir.uppercase()), sortBy)
        val pageable = PageRequest.of(page, size, sort)
        val orders = orderQueryService.getOrdersByCustomer(customerId, pageable)

        return ResponseEntity.ok(
            PagedResponse(
                content = orders.content.map { OrderSummaryResponse.from(it) },
                page = orders.number,
                size = orders.size,
                totalElements = orders.totalElements,
                totalPages = orders.totalPages,
                isFirst = orders.isFirst,
                isLast = orders.isLast
            )
        )
    }

    @GetMapping("/customer/{customerId}/stats")
    fun getCustomerStats(@PathVariable customerId: String): ResponseEntity<CustomerOrderStatsResponse> {
        val stats = orderQueryService.getCustomerStats(customerId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(CustomerOrderStatsResponse.from(stats))
    }

    // Search Endpoint

    @GetMapping
    fun searchOrders(
        @RequestParam(required = false) status: String?,
        @RequestParam(required = false) customerId: String?,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDate: LocalDateTime?,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDate: LocalDateTime?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestParam(defaultValue = "createdAt") sortBy: String,
        @RequestParam(defaultValue = "DESC") sortDir: String
    ): ResponseEntity<PagedResponse<OrderSummaryResponse>> {
        val sort = Sort.by(Sort.Direction.valueOf(sortDir.uppercase()), sortBy)
        val pageable = PageRequest.of(page, size, sort)
        val orders = orderQueryService.searchOrders(status, customerId, startDate, endDate, pageable)

        return ResponseEntity.ok(
            PagedResponse(
                content = orders.content.map { OrderSummaryResponse.from(it) },
                page = orders.number,
                size = orders.size,
                totalElements = orders.totalElements,
                totalPages = orders.totalPages,
                isFirst = orders.isFirst,
                isLast = orders.isLast
            )
        )
    }

    // Admin Event Store Endpoints

    @GetMapping("/{orderId}/events")
    fun getOrderEvents(@PathVariable orderId: String): ResponseEntity<List<EventStoreResponse>> {
        val events = orderQueryService.getOrderEvents(orderId)
        return ResponseEntity.ok(events.map { EventStoreResponse.from(it) })
    }

    @GetMapping("/saga/{sagaId}/events")
    fun getSagaEvents(@PathVariable sagaId: String): ResponseEntity<List<EventStoreResponse>> {
        val events = orderQueryService.getSagaEvents(sagaId)
        return ResponseEntity.ok(events.map { EventStoreResponse.from(it) })
    }

    // Top Customers Endpoints

    @GetMapping("/stats/top-customers/spending")
    fun getTopCustomersBySpending(
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "10") size: Int
    ): ResponseEntity<PagedResponse<CustomerOrderStatsResponse>> {
        val pageable = PageRequest.of(page, size)
        val customers = orderQueryService.getTopCustomersBySpending(pageable)

        return ResponseEntity.ok(
            PagedResponse(
                content = customers.content.map { CustomerOrderStatsResponse.from(it) },
                page = customers.number,
                size = customers.size,
                totalElements = customers.totalElements,
                totalPages = customers.totalPages,
                isFirst = customers.isFirst,
                isLast = customers.isLast
            )
        )
    }

    @GetMapping("/stats/top-customers/orders")
    fun getTopCustomersByOrderCount(
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "10") size: Int
    ): ResponseEntity<PagedResponse<CustomerOrderStatsResponse>> {
        val pageable = PageRequest.of(page, size)
        val customers = orderQueryService.getTopCustomersByOrderCount(pageable)

        return ResponseEntity.ok(
            PagedResponse(
                content = customers.content.map { CustomerOrderStatsResponse.from(it) },
                page = customers.number,
                size = customers.size,
                totalElements = customers.totalElements,
                totalPages = customers.totalPages,
                isFirst = customers.isFirst,
                isLast = customers.isLast
            )
        )
    }
}
