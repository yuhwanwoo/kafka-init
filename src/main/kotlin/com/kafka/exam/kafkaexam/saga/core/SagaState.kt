package com.kafka.exam.kafkaexam.saga.core

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "saga_state")
class SagaState(
    @Id
    val sagaId: String = UUID.randomUUID().toString(),

    @Column(nullable = false)
    val orderId: String,

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
    var status: SagaStatus = SagaStatus.STARTED,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var currentStep: SagaStep = SagaStep.CREATE_ORDER,

    var paymentId: String? = null,

    var failureReason: String? = null,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var updatedAt: LocalDateTime = LocalDateTime.now(),

    var completedAt: LocalDateTime? = null
) {
    fun advanceToNextStep() {
        currentStep.nextStep()?.let {
            currentStep = it
            updatedAt = LocalDateTime.now()
        }
    }

    fun startCompensation(reason: String) {
        status = SagaStatus.COMPENSATING
        failureReason = reason
        updatedAt = LocalDateTime.now()
    }

    fun markCompleted() {
        status = SagaStatus.COMPLETED
        completedAt = LocalDateTime.now()
        updatedAt = LocalDateTime.now()
    }

    fun markCompensated() {
        status = SagaStatus.COMPENSATED
        completedAt = LocalDateTime.now()
        updatedAt = LocalDateTime.now()
    }

    fun markFailed(reason: String) {
        status = SagaStatus.FAILED
        failureReason = reason
        completedAt = LocalDateTime.now()
        updatedAt = LocalDateTime.now()
    }
}
