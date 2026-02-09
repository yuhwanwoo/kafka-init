package com.kafka.exam.kafkaexam.order.controller.dto

import com.kafka.exam.kafkaexam.order.domain.Order
import com.kafka.exam.kafkaexam.order.domain.OrderStatus
import com.kafka.exam.kafkaexam.saga.core.SagaState
import com.kafka.exam.kafkaexam.saga.core.SagaStatus
import java.math.BigDecimal
import java.time.LocalDateTime

data class CreateOrderRequest(
    val customerId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: BigDecimal
)

data class OrderResponse(
    val orderId: String,
    val sagaId: String,
    val customerId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: BigDecimal,
    val status: OrderStatus,
    val createdAt: LocalDateTime
) {
    companion object {
        fun from(order: Order) = OrderResponse(
            orderId = order.orderId,
            sagaId = order.sagaId,
            customerId = order.customerId,
            productId = order.productId,
            quantity = order.quantity,
            totalAmount = order.totalAmount,
            status = order.status,
            createdAt = order.createdAt
        )
    }
}

data class SagaStateResponse(
    val sagaId: String,
    val orderId: String,
    val customerId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: BigDecimal,
    val status: SagaStatus,
    val currentStep: String,
    val paymentId: String?,
    val failureReason: String?,
    val createdAt: LocalDateTime,
    val completedAt: LocalDateTime?
) {
    companion object {
        fun from(state: SagaState) = SagaStateResponse(
            sagaId = state.sagaId,
            orderId = state.orderId,
            customerId = state.customerId,
            productId = state.productId,
            quantity = state.quantity,
            totalAmount = state.totalAmount,
            status = state.status,
            currentStep = state.currentStep.name,
            paymentId = state.paymentId,
            failureReason = state.failureReason,
            createdAt = state.createdAt,
            completedAt = state.completedAt
        )
    }
}
