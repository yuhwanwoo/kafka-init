package com.kafka.exam.kafkaexam.order.query.model

enum class OrderHistoryEventType {
    ORDER_CREATED,
    ORDER_CANCELLED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    PAYMENT_CANCELLED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    INVENTORY_RELEASED,
    ORDER_CONFIRMED,
    ORDER_FAILED
}
