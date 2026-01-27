package com.kafka.exam.kafkaexam.outbox

import java.time.LocalDateTime
import java.util.UUID

data class Outbox(
    val id: String = UUID.randomUUID().toString(),
    val aggregateType: String,        // 예: "Product", "Order"
    val aggregateId: String,          // 예: productId, orderId
    val eventType: String,            // 예: "ProductCreated", "OrderPlaced"
    val payload: String,              // JSON 페이로드
    val topic: String,                // Kafka 토픽
    val status: OutboxStatus = OutboxStatus.PENDING,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val processedAt: LocalDateTime? = null,
    val retryCount: Int = 0
)

enum class OutboxStatus {
    PENDING,    // 발행 대기
    SENT,       // 발행 완료
    FAILED      // 발행 실패
}