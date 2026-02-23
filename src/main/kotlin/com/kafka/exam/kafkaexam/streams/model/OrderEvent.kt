package com.kafka.exam.kafkaexam.streams.model

import java.math.BigDecimal
import java.time.Instant

data class OrderEvent(
    val orderId: String,
    val productId: String,
    val productName: String,
    val quantity: Int,
    val price: BigDecimal,
    val customerId: String,
    val eventType: OrderEventType,
    val timestamp: Instant = Instant.now()
) {
    val totalAmount: BigDecimal
        get() = price.multiply(BigDecimal(quantity))
}

enum class OrderEventType {
    CREATED,
    CONFIRMED,
    CANCELLED,
    REFUNDED
}