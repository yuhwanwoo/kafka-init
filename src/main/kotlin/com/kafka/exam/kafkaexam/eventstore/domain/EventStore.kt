package com.kafka.exam.kafkaexam.eventstore.domain

import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(
    name = "event_store",
    indexes = [
        Index(name = "idx_event_store_aggregate", columnList = "aggregateType, aggregateId"),
        Index(name = "idx_event_store_saga", columnList = "sagaId"),
        Index(name = "idx_event_store_timestamp", columnList = "timestamp")
    ]
)
class EventStore(
    @Id
    val eventId: String = UUID.randomUUID().toString(),

    @Column(nullable = false)
    val aggregateType: String,

    @Column(nullable = false)
    val aggregateId: String,

    @Column(nullable = false)
    val sagaId: String,

    @Column(nullable = false)
    val eventType: String,

    @Column(nullable = false, columnDefinition = "TEXT")
    val payload: String,

    @Column(nullable = false)
    val version: Long = 1,

    @Column(nullable = false)
    val timestamp: LocalDateTime = LocalDateTime.now()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EventStore) return false
        return eventId == other.eventId
    }

    override fun hashCode(): Int = eventId.hashCode()
}
