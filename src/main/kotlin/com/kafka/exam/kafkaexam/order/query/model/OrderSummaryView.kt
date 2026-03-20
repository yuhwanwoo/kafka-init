package com.kafka.exam.kafkaexam.order.query.model

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(
    name = "order_summary_view",
    indexes = [
        Index(name = "idx_order_summary_saga", columnList = "sagaId"),
        Index(name = "idx_order_summary_customer", columnList = "customerId"),
        Index(name = "idx_order_summary_status", columnList = "orderStatus"),
        Index(name = "idx_order_summary_created", columnList = "createdAt")
    ]
)
class OrderSummaryView(
    @Id
    val orderId: String,

    @Column(nullable = false)
    val sagaId: String,

    @Column(nullable = false)
    val customerId: String,

    @Column(nullable = false)
    val productId: String,

    @Column(nullable = false)
    val quantity: Int,

    @Column(nullable = false, precision = 19, scale = 2)
    val totalAmount: BigDecimal,

    @Column(nullable = false)
    var orderStatus: String,

    var paymentId: String? = null,

    var paymentStatus: String? = null,

    @Column(precision = 19, scale = 2)
    var paidAmount: BigDecimal? = null,

    var inventoryReserved: Boolean = false,

    var failureReason: String? = null,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var updatedAt: LocalDateTime = LocalDateTime.now(),

    var completedAt: LocalDateTime? = null
) {
    fun updateOrderStatus(status: String) {
        this.orderStatus = status
        this.updatedAt = LocalDateTime.now()
    }

    fun updatePaymentInfo(paymentId: String, status: String, amount: BigDecimal) {
        this.paymentId = paymentId
        this.paymentStatus = status
        this.paidAmount = amount
        this.updatedAt = LocalDateTime.now()
    }

    fun updateInventoryReserved(reserved: Boolean) {
        this.inventoryReserved = reserved
        this.updatedAt = LocalDateTime.now()
    }

    fun markCompleted() {
        this.orderStatus = "CONFIRMED"
        this.completedAt = LocalDateTime.now()
        this.updatedAt = LocalDateTime.now()
    }

    fun markFailed(reason: String) {
        this.orderStatus = "FAILED"
        this.failureReason = reason
        this.updatedAt = LocalDateTime.now()
    }

    fun markCancelled(reason: String) {
        this.orderStatus = "CANCELLED"
        this.failureReason = reason
        this.updatedAt = LocalDateTime.now()
    }
}
