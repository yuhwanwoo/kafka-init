package com.kafka.exam.kafkaexam.order.service

import com.kafka.exam.kafkaexam.order.domain.Order
import com.kafka.exam.kafkaexam.order.domain.OrderRepository
import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.saga.command.CreateOrderCommand
import com.kafka.exam.kafkaexam.saga.command.CancelOrderCommand
import com.kafka.exam.kafkaexam.saga.event.OrderCreatedEvent
import com.kafka.exam.kafkaexam.saga.event.OrderCancelledEvent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class OrderService(
    private val orderRepository: OrderRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val SAGA_EVENT_TOPIC = "saga-event-topic"
    }

    @Transactional
    fun createOrder(command: CreateOrderCommand): Order {
        log.info("Creating order for sagaId={}, orderId={}", command.sagaId, command.orderId)

        val order = Order(
            orderId = command.orderId,
            sagaId = command.sagaId,
            customerId = command.customerId,
            productId = command.productId,
            quantity = command.quantity,
            totalAmount = command.totalAmount
        )
        orderRepository.save(order)

        val event = OrderCreatedEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            customerId = command.customerId,
            productId = command.productId,
            quantity = command.quantity,
            totalAmount = command.totalAmount
        )

        val outbox = Outbox(
            aggregateType = "Order",
            aggregateId = command.orderId,
            eventType = OutboxEventType.ORDER_CREATED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)

        log.info("Order created: orderId={}", order.orderId)
        return order
    }

    @Transactional
    fun cancelOrder(command: CancelOrderCommand) {
        log.info("Cancelling order for sagaId={}, orderId={}", command.sagaId, command.orderId)

        val order = orderRepository.findById(command.orderId)
            .orElseThrow { IllegalArgumentException("Order not found: ${command.orderId}") }

        order.cancel(command.reason)

        val event = OrderCancelledEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            reason = command.reason
        )

        val outbox = Outbox(
            aggregateType = "Order",
            aggregateId = command.orderId,
            eventType = OutboxEventType.ORDER_CANCELLED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)

        log.info("Order cancelled: orderId={}", order.orderId)
    }

    @Transactional
    fun confirmOrder(orderId: String) {
        val order = orderRepository.findById(orderId)
            .orElseThrow { IllegalArgumentException("Order not found: $orderId") }
        order.confirm()
        log.info("Order confirmed: orderId={}", orderId)
    }

    fun findByOrderId(orderId: String): Order? {
        return orderRepository.findById(orderId).orElse(null)
    }

    fun findBySagaId(sagaId: String): Order? {
        return orderRepository.findBySagaId(sagaId)
    }
}
