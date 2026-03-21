package com.kafka.exam.kafkaexam.order.query.repository

import com.kafka.exam.kafkaexam.order.query.model.CustomerOrderStatsView
import com.kafka.exam.kafkaexam.order.query.model.OrderHistoryView
import com.kafka.exam.kafkaexam.order.query.model.OrderSummaryView
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import java.time.LocalDateTime

@Repository
interface OrderSummaryViewRepository : JpaRepository<OrderSummaryView, String> {

    fun findBySagaId(sagaId: String): OrderSummaryView?

    fun findByCustomerId(customerId: String, pageable: Pageable): Page<OrderSummaryView>

    fun findByOrderStatus(status: String, pageable: Pageable): Page<OrderSummaryView>

    fun findByCustomerIdAndOrderStatus(
        customerId: String,
        status: String,
        pageable: Pageable
    ): Page<OrderSummaryView>

    fun findByCreatedAtBetween(
        startDate: LocalDateTime,
        endDate: LocalDateTime,
        pageable: Pageable
    ): Page<OrderSummaryView>

    fun findByOrderStatusAndCreatedAtBetween(
        status: String,
        startDate: LocalDateTime,
        endDate: LocalDateTime,
        pageable: Pageable
    ): Page<OrderSummaryView>

    @Query("""
        SELECT o FROM OrderSummaryView o
        WHERE (:status IS NULL OR o.orderStatus = :status)
        AND (:customerId IS NULL OR o.customerId = :customerId)
        AND (:startDate IS NULL OR o.createdAt >= :startDate)
        AND (:endDate IS NULL OR o.createdAt <= :endDate)
    """)
    fun findWithFilters(
        status: String?,
        customerId: String?,
        startDate: LocalDateTime?,
        endDate: LocalDateTime?,
        pageable: Pageable
    ): Page<OrderSummaryView>

    fun countByCustomerId(customerId: String): Long

    fun countByOrderStatus(status: String): Long
}

@Repository
interface OrderHistoryViewRepository : JpaRepository<OrderHistoryView, String> {

    fun findByOrderIdOrderByTimestampAsc(orderId: String): List<OrderHistoryView>

    fun findBySagaIdOrderByTimestampAsc(sagaId: String): List<OrderHistoryView>

    fun findByOrderIdOrderByTimestampDesc(orderId: String, pageable: Pageable): Page<OrderHistoryView>

    fun countByOrderId(orderId: String): Long
}

@Repository
interface CustomerOrderStatsViewRepository : JpaRepository<CustomerOrderStatsView, String> {

    @Query("SELECT c FROM CustomerOrderStatsView c ORDER BY c.totalSpent DESC")
    fun findTopCustomersBySpending(pageable: Pageable): Page<CustomerOrderStatsView>

    @Query("SELECT c FROM CustomerOrderStatsView c ORDER BY c.totalOrders DESC")
    fun findTopCustomersByOrderCount(pageable: Pageable): Page<CustomerOrderStatsView>

    @Query("SELECT c FROM CustomerOrderStatsView c WHERE c.lastOrderAt >= :since")
    fun findActiveCustomersSince(since: LocalDateTime, pageable: Pageable): Page<CustomerOrderStatsView>
}
