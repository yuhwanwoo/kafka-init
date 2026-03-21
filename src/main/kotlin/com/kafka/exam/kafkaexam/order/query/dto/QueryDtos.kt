package com.kafka.exam.kafkaexam.order.query.dto

import com.kafka.exam.kafkaexam.eventstore.domain.EventStore
import com.kafka.exam.kafkaexam.order.query.model.CustomerOrderStatsView
import com.kafka.exam.kafkaexam.order.query.model.OrderHistoryEventType
import com.kafka.exam.kafkaexam.order.query.model.OrderHistoryView
import com.kafka.exam.kafkaexam.order.query.model.OrderSummaryView
import java.math.BigDecimal
import java.time.LocalDateTime

// Order Summary Response DTOs

data class OrderSummaryResponse(
    val orderId: String,
    val sagaId: String,
    val customerId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: BigDecimal,
    val orderStatus: String,
    val paymentInfo: PaymentInfoDto?,
    val inventoryReserved: Boolean,
    val failureReason: String?,
    val createdAt: LocalDateTime,
    val updatedAt: LocalDateTime,
    val completedAt: LocalDateTime?
) {
    companion object {
        fun from(view: OrderSummaryView): OrderSummaryResponse {
            return OrderSummaryResponse(
                orderId = view.orderId,
                sagaId = view.sagaId,
                customerId = view.customerId,
                productId = view.productId,
                quantity = view.quantity,
                totalAmount = view.totalAmount,
                orderStatus = view.orderStatus,
                paymentInfo = if (view.paymentId != null) {
                    PaymentInfoDto(
                        paymentId = view.paymentId!!,
                        status = view.paymentStatus ?: "UNKNOWN",
                        paidAmount = view.paidAmount
                    )
                } else null,
                inventoryReserved = view.inventoryReserved,
                failureReason = view.failureReason,
                createdAt = view.createdAt,
                updatedAt = view.updatedAt,
                completedAt = view.completedAt
            )
        }
    }
}

data class PaymentInfoDto(
    val paymentId: String,
    val status: String,
    val paidAmount: BigDecimal?
)

// Order History Response DTOs

data class OrderHistoryResponse(
    val historyId: String,
    val orderId: String,
    val sagaId: String,
    val eventType: OrderHistoryEventType,
    val description: String?,
    val timestamp: LocalDateTime
) {
    companion object {
        fun from(view: OrderHistoryView): OrderHistoryResponse {
            return OrderHistoryResponse(
                historyId = view.historyId,
                orderId = view.orderId,
                sagaId = view.sagaId,
                eventType = view.eventType,
                description = view.description,
                timestamp = view.timestamp
            )
        }
    }
}

data class OrderHistoryDetailResponse(
    val historyId: String,
    val orderId: String,
    val sagaId: String,
    val eventType: OrderHistoryEventType,
    val eventData: String?,
    val description: String?,
    val timestamp: LocalDateTime
) {
    companion object {
        fun from(view: OrderHistoryView): OrderHistoryDetailResponse {
            return OrderHistoryDetailResponse(
                historyId = view.historyId,
                orderId = view.orderId,
                sagaId = view.sagaId,
                eventType = view.eventType,
                eventData = view.eventData,
                description = view.description,
                timestamp = view.timestamp
            )
        }
    }
}

// Customer Stats Response DTOs

data class CustomerOrderStatsResponse(
    val customerId: String,
    val totalOrders: Long,
    val completedOrders: Long,
    val cancelledOrders: Long,
    val failedOrders: Long,
    val totalSpent: BigDecimal,
    val totalQuantity: Long,
    val averageOrderValue: BigDecimal,
    val successRate: Double,
    val lastOrderAt: LocalDateTime?,
    val updatedAt: LocalDateTime
) {
    companion object {
        fun from(view: CustomerOrderStatsView): CustomerOrderStatsResponse {
            return CustomerOrderStatsResponse(
                customerId = view.customerId,
                totalOrders = view.totalOrders,
                completedOrders = view.completedOrders,
                cancelledOrders = view.cancelledOrders,
                failedOrders = view.failedOrders,
                totalSpent = view.totalSpent,
                totalQuantity = view.totalQuantity,
                averageOrderValue = view.getAverageOrderValue(),
                successRate = view.getSuccessRate(),
                lastOrderAt = view.lastOrderAt,
                updatedAt = view.updatedAt
            )
        }
    }
}

// Event Store Response DTOs (Admin)

data class EventStoreResponse(
    val eventId: String,
    val aggregateType: String,
    val aggregateId: String,
    val sagaId: String,
    val eventType: String,
    val payload: String,
    val version: Long,
    val timestamp: LocalDateTime
) {
    companion object {
        fun from(event: EventStore): EventStoreResponse {
            return EventStoreResponse(
                eventId = event.eventId,
                aggregateType = event.aggregateType,
                aggregateId = event.aggregateId,
                sagaId = event.sagaId,
                eventType = event.eventType,
                payload = event.payload,
                version = event.version,
                timestamp = event.timestamp
            )
        }
    }
}

// Page Response Wrapper

data class PagedResponse<T>(
    val content: List<T>,
    val page: Int,
    val size: Int,
    val totalElements: Long,
    val totalPages: Int,
    val isFirst: Boolean,
    val isLast: Boolean
)

// Order Search Request

data class OrderSearchRequest(
    val status: String? = null,
    val customerId: String? = null,
    val startDate: LocalDateTime? = null,
    val endDate: LocalDateTime? = null
)
