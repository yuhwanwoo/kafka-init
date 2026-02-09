package com.kafka.exam.kafkaexam.saga.event

import java.math.BigDecimal
import java.time.LocalDateTime

sealed class SagaEvent {
    abstract val sagaId: String
    abstract val orderId: String
    abstract val timestamp: LocalDateTime
}

// Order Events
data class OrderCreatedEvent(
    override val sagaId: String,
    override val orderId: String,
    val customerId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: BigDecimal,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

data class OrderCancelledEvent(
    override val sagaId: String,
    override val orderId: String,
    val reason: String,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

// Payment Events
data class PaymentCompletedEvent(
    override val sagaId: String,
    override val orderId: String,
    val paymentId: String,
    val amount: BigDecimal,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

data class PaymentFailedEvent(
    override val sagaId: String,
    override val orderId: String,
    val reason: String,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

data class PaymentCancelledEvent(
    override val sagaId: String,
    override val orderId: String,
    val paymentId: String,
    val reason: String,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

// Inventory Events
data class InventoryReservedEvent(
    override val sagaId: String,
    override val orderId: String,
    val productId: String,
    val quantity: Int,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

data class InventoryFailedEvent(
    override val sagaId: String,
    override val orderId: String,
    val productId: String,
    val requestedQuantity: Int,
    val availableQuantity: Int,
    val reason: String,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()

data class InventoryReleasedEvent(
    override val sagaId: String,
    override val orderId: String,
    val productId: String,
    val quantity: Int,
    override val timestamp: LocalDateTime = LocalDateTime.now()
) : SagaEvent()
