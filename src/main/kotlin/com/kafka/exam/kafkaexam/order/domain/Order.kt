package com.kafka.exam.kafkaexam.order.domain

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "orders")
class Order(
    @Id
    val orderId: String = UUID.randomUUID().toString(),

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

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var status: OrderStatus = OrderStatus.PENDING,

    var cancellationReason: String? = null,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun confirm() {
        status = OrderStatus.CONFIRMED
        updatedAt = LocalDateTime.now()
    }

    fun cancel(reason: String) {
        status = OrderStatus.CANCELLED
        cancellationReason = reason
        updatedAt = LocalDateTime.now()
    }

    fun markFailed(reason: String) {
        status = OrderStatus.FAILED
        cancellationReason = reason
        updatedAt = LocalDateTime.now()
    }
}
