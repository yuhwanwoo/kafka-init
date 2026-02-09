package com.kafka.exam.kafkaexam.saga.consumer

import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.saga.event.*
import com.kafka.exam.kafkaexam.saga.orchestrator.OrderSagaOrchestrator
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class SagaEventConsumer(
    private val orchestrator: OrderSagaOrchestrator,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${kafka.saga.event-topic:saga-event-topic}"], groupId = "\${spring.kafka.consumer.group-id}")
    fun consumeEvent(message: String) {
        log.info("Received saga event: {}", message)

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

            val event: SagaEvent? = when (eventType) {
                OutboxEventType.ORDER_CREATED ->
                    objectMapper.readValue(payload, OrderCreatedEvent::class.java)
                OutboxEventType.ORDER_CANCELLED ->
                    objectMapper.readValue(payload, OrderCancelledEvent::class.java)
                OutboxEventType.PAYMENT_COMPLETED ->
                    objectMapper.readValue(payload, PaymentCompletedEvent::class.java)
                OutboxEventType.PAYMENT_FAILED ->
                    objectMapper.readValue(payload, PaymentFailedEvent::class.java)
                OutboxEventType.PAYMENT_CANCELLED ->
                    objectMapper.readValue(payload, PaymentCancelledEvent::class.java)
                OutboxEventType.INVENTORY_RESERVED ->
                    objectMapper.readValue(payload, InventoryReservedEvent::class.java)
                OutboxEventType.INVENTORY_FAILED ->
                    objectMapper.readValue(payload, InventoryFailedEvent::class.java)
                OutboxEventType.INVENTORY_RELEASED ->
                    objectMapper.readValue(payload, InventoryReleasedEvent::class.java)
                else -> {
                    log.warn("Unhandled event type: {}", eventType)
                    null
                }
            }

            event?.let {
                orchestrator.handleEvent(it)
            }
        } catch (e: Exception) {
            log.error("Error processing saga event: {}", e.message, e)
        }
    }
}
