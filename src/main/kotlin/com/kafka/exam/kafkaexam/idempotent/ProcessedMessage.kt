package com.kafka.exam.kafkaexam.idempotent

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(
    name = "processed_messages",
    indexes = [
        Index(name = "idx_processed_messages_expired", columnList = "expiredAt")
    ]
)
class ProcessedMessage(
    @Id
    @Column(length = 255)
    val messageId: String,

    @Column(nullable = false)
    val topic: String,

    @Column(nullable = false)
    val partitionId: Int,

    @Column(nullable = false)
    val offsetId: Long,

    val messageKey: String?,

    @Column(nullable = false)
    val processedAt: LocalDateTime = LocalDateTime.now(),

    @Column(nullable = false)
    val expiredAt: LocalDateTime = LocalDateTime.now().plusDays(7)
)