package com.kafka.exam.kafkaexam.saga.command

import java.math.BigDecimal

sealed class SagaCommand {
    abstract val sagaId: String
    abstract val orderId: String
}

data class CreateOrderCommand(
    override val sagaId: String,
    override val orderId: String,
    val customerId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: BigDecimal
) : SagaCommand()

data class CancelOrderCommand(
    override val sagaId: String,
    override val orderId: String,
    val reason: String
) : SagaCommand()

data class ProcessPaymentCommand(
    override val sagaId: String,
    override val orderId: String,
    val customerId: String,
    val amount: BigDecimal
) : SagaCommand()

data class CancelPaymentCommand(
    override val sagaId: String,
    override val orderId: String,
    val paymentId: String,
    val reason: String
) : SagaCommand()

data class ReserveInventoryCommand(
    override val sagaId: String,
    override val orderId: String,
    val productId: String,
    val quantity: Int
) : SagaCommand()

data class ReleaseInventoryCommand(
    override val sagaId: String,
    override val orderId: String,
    val productId: String,
    val quantity: Int,
    val reason: String
) : SagaCommand()
