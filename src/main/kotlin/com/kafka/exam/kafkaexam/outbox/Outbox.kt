package com.kafka.exam.kafkaexam.outbox

import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "outbox")
class Outbox(
    @Id
    val id: String = UUID.randomUUID().toString(),

    @Column(nullable = false)
    val aggregateType: String,        // 예: "Product", "Order"

    @Column(nullable = false)
    val aggregateId: String,          // 예: productId, orderId

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val eventType: OutboxEventType,

    @Column(columnDefinition = "TEXT", nullable = false)
    val payload: String,              // JSON 페이로드

    @Column(nullable = false)
    val topic: String,                // Kafka 토픽

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var status: OutboxStatus = OutboxStatus.PENDING,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var processedAt: LocalDateTime? = null,

    var retryCount: Int = 0
)

enum class OutboxStatus {
    PENDING,    // 발행 대기
    SENT,       // 발행 완료
    FAILED      // 발행 실패
}