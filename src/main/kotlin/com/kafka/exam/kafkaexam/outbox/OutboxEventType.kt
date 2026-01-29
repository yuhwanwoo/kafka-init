package com.kafka.exam.kafkaexam.outbox

enum class OutboxEventType {
    PRODUCT_REGISTERED,
    PRODUCT_UPDATED,
    PRODUCT_DELETED
}