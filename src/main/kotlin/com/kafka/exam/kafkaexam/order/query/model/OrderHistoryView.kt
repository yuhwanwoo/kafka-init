package com.kafka.exam.kafkaexam.order.query.model

import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(
    name = "order_history_view",
    indexes = [
        Index(name = "idx_order_history_order", columnList = "orderId"),
        Index(name = "idx_order_history_saga", columnList = "sagaId"),
        Index(name = "idx_order_history_timestamp", columnList = "timestamp")
    ]
)
class OrderHistoryView(
    @Id
    val historyId: String = UUID.randomUUID().toString(),

    @Column(nullable = false)
    val orderId: String,

    @Column(nullable = false)
    val sagaId: String,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val eventType: OrderHistoryEventType,

    @Column(columnDefinition = "TEXT")
    val eventData: String? = null,

    val description: String? = null,

    @Column(nullable = false)
    val timestamp: LocalDateTime = LocalDateTime.now()
) {
    companion object {
        fun create(
            orderId: String,
            sagaId: String,
            eventType: OrderHistoryEventType,
            eventData: String? = null,
            description: String? = null
        ): OrderHistoryView {
            return OrderHistoryView(
                orderId = orderId,
                sagaId = sagaId,
                eventType = eventType,
                eventData = eventData,
                description = description
            )
        }
    }
}
