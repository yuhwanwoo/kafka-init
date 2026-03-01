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

    var transactionId: String? = null,

    var paymentKey: String? = null,

    var refundId: String? = null,

    var failureReason: String? = null,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var updatedAt: LocalDateTime = LocalDateTime.now(),

    var completedAt: LocalDateTime? = null,

    var cancelledAt: LocalDateTime? = null
) {
    fun complete(transactionId: String?, paymentKey: String?) {
        this.status = PaymentStatus.COMPLETED
        this.transactionId = transactionId
        this.paymentKey = paymentKey
        this.completedAt = LocalDateTime.now()
        this.updatedAt = LocalDateTime.now()
    }

    fun fail(reason: String) {
        this.status = PaymentStatus.FAILED
        this.failureReason = reason
        this.updatedAt = LocalDateTime.now()
    }

    fun cancel(reason: String, refundId: String?) {
        this.status = PaymentStatus.CANCELLED
        this.failureReason = reason
        this.refundId = refundId
        this.cancelledAt = LocalDateTime.now()
        this.updatedAt = LocalDateTime.now()
    }
}
