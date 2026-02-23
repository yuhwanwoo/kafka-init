package com.kafka.exam.kafkaexam.streams.model

import java.math.BigDecimal

data class OrderStatistics(
    val productId: String,
    val productName: String,
    val totalOrders: Long,
    val totalQuantity: Long,
    val totalRevenue: BigDecimal,
    val cancelledOrders: Long
) {
    companion object {
        fun empty(productId: String, productName: String) = OrderStatistics(
            productId = productId,
            productName = productName,
            totalOrders = 0,
            totalQuantity = 0,
            totalRevenue = BigDecimal.ZERO,
            cancelledOrders = 0
        )
    }

    fun aggregate(event: OrderEvent): OrderStatistics {
        return when (event.eventType) {
            OrderEventType.CREATED, OrderEventType.CONFIRMED -> copy(
                totalOrders = totalOrders + 1,
                totalQuantity = totalQuantity + event.quantity,
                totalRevenue = totalRevenue.add(event.totalAmount)
            )
            OrderEventType.CANCELLED, OrderEventType.REFUNDED -> copy(
                cancelledOrders = cancelledOrders + 1,
                totalRevenue = totalRevenue.subtract(event.totalAmount)
            )
        }
    }
}