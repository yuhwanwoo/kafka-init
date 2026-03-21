package com.kafka.exam.kafkaexam.order.query.model

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "customer_order_stats_view")
class CustomerOrderStatsView(
    @Id
    val customerId: String,

    @Column(nullable = false)
    var totalOrders: Long = 0,

    @Column(nullable = false)
    var completedOrders: Long = 0,

    @Column(nullable = false)
    var cancelledOrders: Long = 0,

    @Column(nullable = false)
    var failedOrders: Long = 0,

    @Column(nullable = false, precision = 19, scale = 2)
    var totalSpent: BigDecimal = BigDecimal.ZERO,

    @Column(nullable = false)
    var totalQuantity: Long = 0,

    var lastOrderAt: LocalDateTime? = null,

    @Column(nullable = false)
    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun incrementTotalOrders() {
        totalOrders++
        updatedAt = LocalDateTime.now()
    }

    fun incrementCompletedOrders(amount: BigDecimal, quantity: Int) {
        completedOrders++
        totalSpent = totalSpent.add(amount)
        totalQuantity += quantity
        updatedAt = LocalDateTime.now()
    }

    fun incrementCancelledOrders() {
        cancelledOrders++
        updatedAt = LocalDateTime.now()
    }

    fun incrementFailedOrders() {
        failedOrders++
        updatedAt = LocalDateTime.now()
    }

    fun updateLastOrderAt(timestamp: LocalDateTime) {
        if (lastOrderAt == null || timestamp.isAfter(lastOrderAt)) {
            lastOrderAt = timestamp
            updatedAt = LocalDateTime.now()
        }
    }

    fun getAverageOrderValue(): BigDecimal {
        return if (completedOrders > 0) {
            totalSpent.divide(BigDecimal(completedOrders), 2, java.math.RoundingMode.HALF_UP)
        } else {
            BigDecimal.ZERO
        }
    }

    fun getSuccessRate(): Double {
        return if (totalOrders > 0) {
            completedOrders.toDouble() / totalOrders.toDouble() * 100
        } else {
            0.0
        }
    }

    companion object {
        fun createNew(customerId: String): CustomerOrderStatsView {
            return CustomerOrderStatsView(customerId = customerId)
        }
    }
}
