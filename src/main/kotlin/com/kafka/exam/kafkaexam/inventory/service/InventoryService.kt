package com.kafka.exam.kafkaexam.inventory.service

import com.kafka.exam.kafkaexam.inventory.domain.Inventory
import com.kafka.exam.kafkaexam.inventory.domain.InventoryRepository
import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.saga.command.ReleaseInventoryCommand
import com.kafka.exam.kafkaexam.saga.command.ReserveInventoryCommand
import com.kafka.exam.kafkaexam.saga.event.InventoryFailedEvent
import com.kafka.exam.kafkaexam.saga.event.InventoryReleasedEvent
import com.kafka.exam.kafkaexam.saga.event.InventoryReservedEvent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class InventoryService(
    private val inventoryRepository: InventoryRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val SAGA_EVENT_TOPIC = "saga-event-topic"
    }

    @Transactional
    fun reserveStock(command: ReserveInventoryCommand) {
        log.info("Reserving stock for sagaId={}, productId={}, quantity={}",
            command.sagaId, command.productId, command.quantity)

        val inventory = inventoryRepository.findById(command.productId).orElse(null)

        if (inventory == null) {
            log.warn("Inventory not found for productId={}", command.productId)
            publishInventoryFailed(command, 0, "Inventory not found")
            return
        }

        val availableQty = inventory.availableQuantity()
        if (!inventory.reserve(command.quantity)) {
            log.warn("Insufficient stock for productId={}, requested={}, available={}",
                command.productId, command.quantity, availableQty)
            publishInventoryFailed(command, availableQty, "Insufficient stock")
            return
        }

        inventoryRepository.save(inventory)

        val event = InventoryReservedEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            productId = command.productId,
            quantity = command.quantity
        )

        val outbox = Outbox(
            aggregateType = "Inventory",
            aggregateId = command.productId,
            eventType = OutboxEventType.INVENTORY_RESERVED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)

        log.info("Stock reserved: productId={}, quantity={}", command.productId, command.quantity)
    }

    private fun publishInventoryFailed(command: ReserveInventoryCommand, availableQty: Int, reason: String) {
        val event = InventoryFailedEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            productId = command.productId,
            requestedQuantity = command.quantity,
            availableQuantity = availableQty,
            reason = reason
        )

        val outbox = Outbox(
            aggregateType = "Inventory",
            aggregateId = command.productId,
            eventType = OutboxEventType.INVENTORY_FAILED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)
    }

    @Transactional
    fun releaseStock(command: ReleaseInventoryCommand) {
        log.info("Releasing stock for sagaId={}, productId={}, quantity={}",
            command.sagaId, command.productId, command.quantity)

        val inventory = inventoryRepository.findById(command.productId)
            .orElseThrow { IllegalArgumentException("Inventory not found: ${command.productId}") }

        inventory.release(command.quantity)
        inventoryRepository.save(inventory)

        val event = InventoryReleasedEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            productId = command.productId,
            quantity = command.quantity
        )

        val outbox = Outbox(
            aggregateType = "Inventory",
            aggregateId = command.productId,
            eventType = OutboxEventType.INVENTORY_RELEASED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)

        log.info("Stock released: productId={}, quantity={}", command.productId, command.quantity)
    }

    @Transactional
    fun initializeInventory(productId: String, quantity: Int): Inventory {
        val inventory = Inventory(productId = productId, quantity = quantity)
        return inventoryRepository.save(inventory)
    }

    fun findByProductId(productId: String): Inventory? {
        return inventoryRepository.findById(productId).orElse(null)
    }
}
