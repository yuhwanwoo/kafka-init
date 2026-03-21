package com.kafka.exam.kafkaexam.order.query.projector

import com.kafka.exam.kafkaexam.eventstore.service.EventStoreService
import com.kafka.exam.kafkaexam.order.query.model.CustomerOrderStatsView
import com.kafka.exam.kafkaexam.order.query.model.OrderHistoryEventType
import com.kafka.exam.kafkaexam.order.query.model.OrderHistoryView
import com.kafka.exam.kafkaexam.order.query.model.OrderSummaryView
import com.kafka.exam.kafkaexam.order.query.repository.CustomerOrderStatsViewRepository
import com.kafka.exam.kafkaexam.order.query.repository.OrderHistoryViewRepository
import com.kafka.exam.kafkaexam.order.query.repository.OrderSummaryViewRepository
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.saga.event.*
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Component
class OrderEventProjector(
    private val orderSummaryViewRepository: OrderSummaryViewRepository,
    private val orderHistoryViewRepository: OrderHistoryViewRepository,
    private val customerOrderStatsViewRepository: CustomerOrderStatsViewRepository,
    private val eventStoreService: EventStoreService,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["\${kafka.saga.event-topic:saga-event-topic}"],
        groupId = "cqrs-projector-group"
    )
    @Transactional
    fun projectEvent(message: String) {
        log.debug("Received event for projection: {}", message)

        try {
            val node = objectMapper.readTree(message)
            val eventTypeStr = node.get("eventType")?.textValue()
            val payload = node.get("payload")?.textValue() ?: return

            if (eventTypeStr == null) {
                log.warn("Event type not found in message")
                return
            }

            val eventType = try {
                OutboxEventType.valueOf(eventTypeStr)
            } catch (e: IllegalArgumentException) {
                log.warn("Unknown event type: {}", eventTypeStr)
                return
            }

            when (eventType) {
                OutboxEventType.ORDER_CREATED -> {
                    val event = objectMapper.readValue(payload, OrderCreatedEvent::class.java)
                    handleOrderCreated(event, payload)
                }
                OutboxEventType.ORDER_CANCELLED -> {
                    val event = objectMapper.readValue(payload, OrderCancelledEvent::class.java)
                    handleOrderCancelled(event, payload)
                }
                OutboxEventType.PAYMENT_COMPLETED -> {
                    val event = objectMapper.readValue(payload, PaymentCompletedEvent::class.java)
                    handlePaymentCompleted(event, payload)
                }
                OutboxEventType.PAYMENT_FAILED -> {
                    val event = objectMapper.readValue(payload, PaymentFailedEvent::class.java)
                    handlePaymentFailed(event, payload)
                }
                OutboxEventType.PAYMENT_CANCELLED -> {
                    val event = objectMapper.readValue(payload, PaymentCancelledEvent::class.java)
                    handlePaymentCancelled(event, payload)
                }
                OutboxEventType.INVENTORY_RESERVED -> {
                    val event = objectMapper.readValue(payload, InventoryReservedEvent::class.java)
                    handleInventoryReserved(event, payload)
                }
                OutboxEventType.INVENTORY_FAILED -> {
                    val event = objectMapper.readValue(payload, InventoryFailedEvent::class.java)
                    handleInventoryFailed(event, payload)
                }
                OutboxEventType.INVENTORY_RELEASED -> {
                    val event = objectMapper.readValue(payload, InventoryReleasedEvent::class.java)
                    handleInventoryReleased(event, payload)
                }
                else -> {
                    log.debug("Event type {} not handled by projector", eventType)
                }
            }
        } catch (e: Exception) {
            log.error("Error projecting event: {}", e.message, e)
        }
    }

    private fun handleOrderCreated(event: OrderCreatedEvent, payload: String) {
        log.info("Projecting OrderCreatedEvent: orderId={}", event.orderId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "ORDER_CREATED",
            payload = payload
        )

        // Create Order Summary View
        val summary = OrderSummaryView(
            orderId = event.orderId,
            sagaId = event.sagaId,
            customerId = event.customerId,
            productId = event.productId,
            quantity = event.quantity,
            totalAmount = event.totalAmount,
            orderStatus = "PENDING",
            createdAt = event.timestamp
        )
        orderSummaryViewRepository.save(summary)

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.ORDER_CREATED,
            eventData = payload,
            description = "Order created with amount ${event.totalAmount} for ${event.quantity} items"
        )
        orderHistoryViewRepository.save(history)

        // Update Customer Stats
        val stats = customerOrderStatsViewRepository.findById(event.customerId)
            .orElse(CustomerOrderStatsView.createNew(event.customerId))
        stats.incrementTotalOrders()
        stats.updateLastOrderAt(event.timestamp)
        customerOrderStatsViewRepository.save(stats)

        log.info("OrderCreatedEvent projected successfully: orderId={}", event.orderId)
    }

    private fun handleOrderCancelled(event: OrderCancelledEvent, payload: String) {
        log.info("Projecting OrderCancelledEvent: orderId={}", event.orderId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "ORDER_CANCELLED",
            payload = payload
        )

        // Update Order Summary View
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.markCancelled(event.reason)
            orderSummaryViewRepository.save(summary)

            // Update Customer Stats
            customerOrderStatsViewRepository.findById(summary.customerId).ifPresent { stats ->
                stats.incrementCancelledOrders()
                customerOrderStatsViewRepository.save(stats)
            }
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.ORDER_CANCELLED,
            eventData = payload,
            description = "Order cancelled: ${event.reason}"
        )
        orderHistoryViewRepository.save(history)
    }

    private fun handlePaymentCompleted(event: PaymentCompletedEvent, payload: String) {
        log.info("Projecting PaymentCompletedEvent: orderId={}, paymentId={}", event.orderId, event.paymentId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "PAYMENT_COMPLETED",
            payload = payload
        )

        // Update Order Summary View
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.updatePaymentInfo(event.paymentId, "COMPLETED", event.amount)
            orderSummaryViewRepository.save(summary)
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.PAYMENT_COMPLETED,
            eventData = payload,
            description = "Payment of ${event.amount} completed (paymentId: ${event.paymentId})"
        )
        orderHistoryViewRepository.save(history)
    }

    private fun handlePaymentFailed(event: PaymentFailedEvent, payload: String) {
        log.info("Projecting PaymentFailedEvent: orderId={}", event.orderId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "PAYMENT_FAILED",
            payload = payload
        )

        // Update Order Summary View
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.paymentStatus = "FAILED"
            summary.markFailed(event.reason)
            orderSummaryViewRepository.save(summary)

            // Update Customer Stats
            customerOrderStatsViewRepository.findById(summary.customerId).ifPresent { stats ->
                stats.incrementFailedOrders()
                customerOrderStatsViewRepository.save(stats)
            }
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.PAYMENT_FAILED,
            eventData = payload,
            description = "Payment failed: ${event.reason}"
        )
        orderHistoryViewRepository.save(history)
    }

    private fun handlePaymentCancelled(event: PaymentCancelledEvent, payload: String) {
        log.info("Projecting PaymentCancelledEvent: orderId={}, paymentId={}", event.orderId, event.paymentId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "PAYMENT_CANCELLED",
            payload = payload
        )

        // Update Order Summary View
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.paymentStatus = "CANCELLED"
            orderSummaryViewRepository.save(summary)
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.PAYMENT_CANCELLED,
            eventData = payload,
            description = "Payment cancelled: ${event.reason}"
        )
        orderHistoryViewRepository.save(history)
    }

    private fun handleInventoryReserved(event: InventoryReservedEvent, payload: String) {
        log.info("Projecting InventoryReservedEvent: orderId={}, productId={}", event.orderId, event.productId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "INVENTORY_RESERVED",
            payload = payload
        )

        // Update Order Summary View - mark as completed
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.updateInventoryReserved(true)
            summary.markCompleted()
            orderSummaryViewRepository.save(summary)

            // Update Customer Stats
            customerOrderStatsViewRepository.findById(summary.customerId).ifPresent { stats ->
                stats.incrementCompletedOrders(summary.totalAmount, summary.quantity)
                customerOrderStatsViewRepository.save(stats)
            }
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.INVENTORY_RESERVED,
            eventData = payload,
            description = "Inventory reserved: ${event.quantity} units of ${event.productId}"
        )
        orderHistoryViewRepository.save(history)

        // Add order confirmed history
        val confirmedHistory = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.ORDER_CONFIRMED,
            description = "Order confirmed successfully"
        )
        orderHistoryViewRepository.save(confirmedHistory)
    }

    private fun handleInventoryFailed(event: InventoryFailedEvent, payload: String) {
        log.info("Projecting InventoryFailedEvent: orderId={}, productId={}", event.orderId, event.productId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "INVENTORY_FAILED",
            payload = payload
        )

        // Update Order Summary View
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.updateInventoryReserved(false)
            summary.markFailed(event.reason)
            orderSummaryViewRepository.save(summary)

            // Update Customer Stats
            customerOrderStatsViewRepository.findById(summary.customerId).ifPresent { stats ->
                stats.incrementFailedOrders()
                customerOrderStatsViewRepository.save(stats)
            }
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.INVENTORY_FAILED,
            eventData = payload,
            description = "Inventory failed: ${event.reason} (requested: ${event.requestedQuantity}, available: ${event.availableQuantity})"
        )
        orderHistoryViewRepository.save(history)
    }

    private fun handleInventoryReleased(event: InventoryReleasedEvent, payload: String) {
        log.info("Projecting InventoryReleasedEvent: orderId={}, productId={}", event.orderId, event.productId)

        // Store in Event Store
        eventStoreService.storeEventFromPayload(
            aggregateType = "Order",
            aggregateId = event.orderId,
            sagaId = event.sagaId,
            eventType = "INVENTORY_RELEASED",
            payload = payload
        )

        // Update Order Summary View
        orderSummaryViewRepository.findById(event.orderId).ifPresent { summary ->
            summary.updateInventoryReserved(false)
            orderSummaryViewRepository.save(summary)
        }

        // Create History Entry
        val history = OrderHistoryView.create(
            orderId = event.orderId,
            sagaId = event.sagaId,
            eventType = OrderHistoryEventType.INVENTORY_RELEASED,
            eventData = payload,
            description = "Inventory released: ${event.quantity} units of ${event.productId}"
        )
        orderHistoryViewRepository.save(history)
    }
}
