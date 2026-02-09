package com.kafka.exam.kafkaexam.saga.consumer

import com.kafka.exam.kafkaexam.inventory.service.InventoryService
import com.kafka.exam.kafkaexam.order.service.OrderService
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.payment.service.PaymentService
import com.kafka.exam.kafkaexam.saga.command.*
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class SagaCommandConsumer(
    private val orderService: OrderService,
    private val paymentService: PaymentService,
    private val inventoryService: InventoryService,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${kafka.saga.command-topic:saga-command-topic}"], groupId = "\${spring.kafka.consumer.group-id}")
    fun consumeCommand(message: String) {
        log.info("Received saga command: {}", message)

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
                OutboxEventType.CREATE_ORDER -> {
                    val command = objectMapper.readValue(payload, CreateOrderCommand::class.java)
                    orderService.createOrder(command)
                }
                OutboxEventType.CANCEL_ORDER -> {
                    val command = objectMapper.readValue(payload, CancelOrderCommand::class.java)
                    orderService.cancelOrder(command)
                }
                OutboxEventType.PROCESS_PAYMENT -> {
                    val command = objectMapper.readValue(payload, ProcessPaymentCommand::class.java)
                    paymentService.processPayment(command)
                }
                OutboxEventType.CANCEL_PAYMENT -> {
                    val command = objectMapper.readValue(payload, CancelPaymentCommand::class.java)
                    paymentService.cancelPayment(command)
                }
                OutboxEventType.RESERVE_INVENTORY -> {
                    val command = objectMapper.readValue(payload, ReserveInventoryCommand::class.java)
                    inventoryService.reserveStock(command)
                }
                OutboxEventType.RELEASE_INVENTORY -> {
                    val command = objectMapper.readValue(payload, ReleaseInventoryCommand::class.java)
                    inventoryService.releaseStock(command)
                }
                else -> log.warn("Unhandled command type: {}", eventType)
            }
        } catch (e: Exception) {
            log.error("Error processing saga command: {}", e.message, e)
        }
    }
}
