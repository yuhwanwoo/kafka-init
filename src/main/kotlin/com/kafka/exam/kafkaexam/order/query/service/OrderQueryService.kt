package com.kafka.exam.kafkaexam.order.query.service

import com.kafka.exam.kafkaexam.eventstore.domain.EventStore
import com.kafka.exam.kafkaexam.eventstore.service.EventStoreService
import com.kafka.exam.kafkaexam.order.query.model.CustomerOrderStatsView
import com.kafka.exam.kafkaexam.order.query.model.OrderHistoryView
import com.kafka.exam.kafkaexam.order.query.model.OrderSummaryView
import com.kafka.exam.kafkaexam.order.query.repository.CustomerOrderStatsViewRepository
import com.kafka.exam.kafkaexam.order.query.repository.OrderHistoryViewRepository
import com.kafka.exam.kafkaexam.order.query.repository.OrderSummaryViewRepository
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
@Transactional(readOnly = true)
class OrderQueryService(
    private val orderSummaryViewRepository: OrderSummaryViewRepository,
    private val orderHistoryViewRepository: OrderHistoryViewRepository,
    private val customerOrderStatsViewRepository: CustomerOrderStatsViewRepository,
    private val eventStoreService: EventStoreService
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // Order Summary Queries

    fun getOrderSummary(orderId: String): OrderSummaryView? {
        log.debug("Getting order summary for orderId={}", orderId)
        return orderSummaryViewRepository.findById(orderId).orElse(null)
    }

    fun getOrderSummaryBySagaId(sagaId: String): OrderSummaryView? {
        log.debug("Getting order summary for sagaId={}", sagaId)
        return orderSummaryViewRepository.findBySagaId(sagaId)
    }

    fun getOrdersByCustomer(customerId: String, pageable: Pageable): Page<OrderSummaryView> {
        log.debug("Getting orders for customerId={}", customerId)
        return orderSummaryViewRepository.findByCustomerId(customerId, pageable)
    }

    fun getOrdersByStatus(status: String, pageable: Pageable): Page<OrderSummaryView> {
        log.debug("Getting orders with status={}", status)
        return orderSummaryViewRepository.findByOrderStatus(status, pageable)
    }

    fun getOrdersByCustomerAndStatus(
        customerId: String,
        status: String,
        pageable: Pageable
    ): Page<OrderSummaryView> {
        log.debug("Getting orders for customerId={} with status={}", customerId, status)
        return orderSummaryViewRepository.findByCustomerIdAndOrderStatus(customerId, status, pageable)
    }

    fun getOrdersByDateRange(
        startDate: LocalDateTime,
        endDate: LocalDateTime,
        pageable: Pageable
    ): Page<OrderSummaryView> {
        log.debug("Getting orders between {} and {}", startDate, endDate)
        return orderSummaryViewRepository.findByCreatedAtBetween(startDate, endDate, pageable)
    }

    fun searchOrders(
        status: String?,
        customerId: String?,
        startDate: LocalDateTime?,
        endDate: LocalDateTime?,
        pageable: Pageable
    ): Page<OrderSummaryView> {
        log.debug(
            "Searching orders with filters: status={}, customerId={}, startDate={}, endDate={}",
            status, customerId, startDate, endDate
        )
        return orderSummaryViewRepository.findWithFilters(status, customerId, startDate, endDate, pageable)
    }

    // Order History Queries

    fun getOrderHistory(orderId: String): List<OrderHistoryView> {
        log.debug("Getting history for orderId={}", orderId)
        return orderHistoryViewRepository.findByOrderIdOrderByTimestampAsc(orderId)
    }

    fun getOrderHistoryBySagaId(sagaId: String): List<OrderHistoryView> {
        log.debug("Getting history for sagaId={}", sagaId)
        return orderHistoryViewRepository.findBySagaIdOrderByTimestampAsc(sagaId)
    }

    fun getOrderHistoryPaged(orderId: String, pageable: Pageable): Page<OrderHistoryView> {
        log.debug("Getting paged history for orderId={}", orderId)
        return orderHistoryViewRepository.findByOrderIdOrderByTimestampDesc(orderId, pageable)
    }

    // Customer Stats Queries

    fun getCustomerStats(customerId: String): CustomerOrderStatsView? {
        log.debug("Getting stats for customerId={}", customerId)
        return customerOrderStatsViewRepository.findById(customerId).orElse(null)
    }

    fun getTopCustomersBySpending(pageable: Pageable): Page<CustomerOrderStatsView> {
        log.debug("Getting top customers by spending")
        return customerOrderStatsViewRepository.findTopCustomersBySpending(pageable)
    }

    fun getTopCustomersByOrderCount(pageable: Pageable): Page<CustomerOrderStatsView> {
        log.debug("Getting top customers by order count")
        return customerOrderStatsViewRepository.findTopCustomersByOrderCount(pageable)
    }

    fun getActiveCustomersSince(since: LocalDateTime, pageable: Pageable): Page<CustomerOrderStatsView> {
        log.debug("Getting active customers since {}", since)
        return customerOrderStatsViewRepository.findActiveCustomersSince(since, pageable)
    }

    // Event Store Queries (Admin)

    fun getOrderEvents(orderId: String): List<EventStore> {
        log.debug("Getting raw events for orderId={}", orderId)
        return eventStoreService.getEventsByAggregate("Order", orderId)
    }

    fun getSagaEvents(sagaId: String): List<EventStore> {
        log.debug("Getting raw events for sagaId={}", sagaId)
        return eventStoreService.getEventsBySagaId(sagaId)
    }

    // Aggregate Queries

    fun getOrderCountByCustomer(customerId: String): Long {
        return orderSummaryViewRepository.countByCustomerId(customerId)
    }

    fun getOrderCountByStatus(status: String): Long {
        return orderSummaryViewRepository.countByOrderStatus(status)
    }

    fun getHistoryCountByOrder(orderId: String): Long {
        return orderHistoryViewRepository.countByOrderId(orderId)
    }
}
