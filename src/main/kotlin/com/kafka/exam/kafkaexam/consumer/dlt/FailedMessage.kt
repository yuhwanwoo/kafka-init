package com.kafka.exam.kafkaexam.consumer.dlt

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "failed_messages")
class FailedMessage(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(nullable = false)
    val originalTopic: String,

    @Column(nullable = false)
    val partitionId: Int,

    @Column(nullable = false)
    val offsetId: Long,

    val messageKey: String?,

    @Column(columnDefinition = "TEXT")
    val messageValue: String?,

    @Column(columnDefinition = "TEXT")
    val errorMessage: String?,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var status: FailedMessageStatus = FailedMessageStatus.PENDING,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var resolvedAt: LocalDateTime? = null,

    var retryCount: Int = 0
)