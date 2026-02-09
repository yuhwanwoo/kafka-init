package com.kafka.exam.kafkaexam.payment.domain

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "payments")
class Payment(
    @Id
    val paymentId: String = UUID.randomUUID().toString(),

    @Column(nullable = false)
    val sagaId: String,

    @Column(nullable = false)
    val orderId: String,

    @Column(nullable = false)
    val customerId: String,

    @Column(nullable = false, precision = 19, scale = 2)
    val amount: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var status: PaymentStatus = PaymentStatus.PENDING,

    var failureReason: String? = null,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun complete() {
        status = PaymentStatus.COMPLETED
        updatedAt = LocalDateTime.now()
    }

    fun fail(reason: String) {
        status = PaymentStatus.FAILED
        failureReason = reason
        updatedAt = LocalDateTime.now()
    }

    fun cancel(reason: String) {
        status = PaymentStatus.CANCELLED
        failureReason = reason
        updatedAt = LocalDateTime.now()
    }
}
