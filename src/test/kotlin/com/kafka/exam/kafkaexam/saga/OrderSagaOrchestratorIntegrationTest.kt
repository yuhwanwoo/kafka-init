package com.kafka.exam.kafkaexam.saga

import com.kafka.exam.kafkaexam.order.domain.Order
import com.kafka.exam.kafkaexam.order.domain.OrderRepository
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.outbox.OutboxStatus
import com.kafka.exam.kafkaexam.saga.core.SagaStateRepository
import com.kafka.exam.kafkaexam.saga.core.SagaStatus
import com.kafka.exam.kafkaexam.saga.core.SagaStep
import com.kafka.exam.kafkaexam.saga.event.*
import com.kafka.exam.kafkaexam.saga.orchestrator.OrderSagaOrchestrator
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(
    partitions = 1,
    topics = ["test-saga-command-topic", "test-saga-event-topic"]
)
class OrderSagaOrchestratorIntegrationTest {

    @Autowired
    private lateinit var orderSagaOrchestrator: OrderSagaOrchestrator

    @Autowired
    private lateinit var sagaStateRepository: SagaStateRepository

    @Autowired
    private lateinit var outboxRepository: OutboxRepository

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Test
    fun `Saga 시작 시 SagaState가 생성되고 CreateOrder 커맨드가 발행된다`() {
        // given
        val customerId = "customer-1"
        val productId = "product-1"
        val quantity = 2
        val totalAmount = BigDecimal("10000")

        // when
        val sagaState = orderSagaOrchestrator.startOrderSaga(customerId, productId, quantity, totalAmount)

        // then
        assertNotNull(sagaState.sagaId)
        assertNotNull(sagaState.orderId)
        assertEquals(customerId, sagaState.customerId)
        assertEquals(productId, sagaState.productId)
        assertEquals(quantity, sagaState.quantity)
        assertEquals(totalAmount, sagaState.totalAmount)
        assertEquals(SagaStatus.ORDER_PENDING, sagaState.status)
        assertEquals(SagaStep.CREATE_ORDER, sagaState.currentStep)

        // SagaState가 저장되었는지 확인
        val savedState = sagaStateRepository.findById(sagaState.sagaId).orElse(null)
        assertNotNull(savedState)

        // CreateOrder 커맨드가 Outbox에 저장되었는지 확인
        val pendingOutbox = outboxRepository.findByStatus(OutboxStatus.PENDING)
        assertEquals(1, pendingOutbox.size)
        assertEquals(OutboxEventType.CREATE_ORDER, pendingOutbox[0].eventType)
        assertEquals(sagaState.sagaId, pendingOutbox[0].aggregateId)
    }

    @Test
    fun `OrderCreatedEvent 처리 시 결제 처리 단계로 진행한다`() {
        // given
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = "customer-1",
            productId = "product-1",
            quantity = 2,
            totalAmount = BigDecimal("10000")
        )
        outboxRepository.deleteAll() // 이전 outbox 클리어

        val event = OrderCreatedEvent(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            customerId = sagaState.customerId,
            productId = sagaState.productId,
            quantity = sagaState.quantity,
            totalAmount = sagaState.totalAmount
        )

        // when
        orderSagaOrchestrator.handleEvent(event)

        // then
        val updatedState = sagaStateRepository.findById(sagaState.sagaId).get()
        assertEquals(SagaStatus.PAYMENT_PENDING, updatedState.status)
        assertEquals(SagaStep.PROCESS_PAYMENT, updatedState.currentStep)

        // ProcessPayment 커맨드가 Outbox에 저장되었는지 확인
        val pendingOutbox = outboxRepository.findByStatus(OutboxStatus.PENDING)
        assertEquals(1, pendingOutbox.size)
        assertEquals(OutboxEventType.PROCESS_PAYMENT, pendingOutbox[0].eventType)
    }

    @Test
    fun `PaymentCompletedEvent 처리 시 재고 예약 단계로 진행한다`() {
        // given
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = "customer-1",
            productId = "product-1",
            quantity = 2,
            totalAmount = BigDecimal("10000")
        )

        // OrderCreated 이벤트 처리
        orderSagaOrchestrator.handleEvent(
            OrderCreatedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                customerId = sagaState.customerId,
                productId = sagaState.productId,
                quantity = sagaState.quantity,
                totalAmount = sagaState.totalAmount
            )
        )
        outboxRepository.deleteAll()

        val paymentId = UUID.randomUUID().toString()
        val event = PaymentCompletedEvent(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            paymentId = paymentId,
            amount = sagaState.totalAmount
        )

        // when
        orderSagaOrchestrator.handleEvent(event)

        // then
        val updatedState = sagaStateRepository.findById(sagaState.sagaId).get()
        assertEquals(SagaStatus.INVENTORY_PENDING, updatedState.status)
        assertEquals(SagaStep.RESERVE_INVENTORY, updatedState.currentStep)
        assertEquals(paymentId, updatedState.paymentId)

        // ReserveInventory 커맨드가 Outbox에 저장되었는지 확인
        val pendingOutbox = outboxRepository.findByStatus(OutboxStatus.PENDING)
        assertEquals(1, pendingOutbox.size)
        assertEquals(OutboxEventType.RESERVE_INVENTORY, pendingOutbox[0].eventType)
    }

    @Test
    fun `InventoryReservedEvent 처리 시 Saga가 완료된다`() {
        // given
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = "customer-1",
            productId = "product-1",
            quantity = 2,
            totalAmount = BigDecimal("10000")
        )

        // Order 엔티티 생성 (실제 환경에서는 SagaCommandConsumer가 생성)
        val order = Order(
            orderId = sagaState.orderId,
            sagaId = sagaState.sagaId,
            customerId = sagaState.customerId,
            productId = sagaState.productId,
            quantity = sagaState.quantity,
            totalAmount = sagaState.totalAmount
        )
        orderRepository.save(order)

        // OrderCreated -> PaymentCompleted 순서로 이벤트 처리
        orderSagaOrchestrator.handleEvent(
            OrderCreatedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                customerId = sagaState.customerId,
                productId = sagaState.productId,
                quantity = sagaState.quantity,
                totalAmount = sagaState.totalAmount
            )
        )

        val paymentId = UUID.randomUUID().toString()
        orderSagaOrchestrator.handleEvent(
            PaymentCompletedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                paymentId = paymentId,
                amount = sagaState.totalAmount
            )
        )

        val event = InventoryReservedEvent(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            productId = sagaState.productId,
            quantity = sagaState.quantity
        )

        // when
        orderSagaOrchestrator.handleEvent(event)

        // then
        val updatedState = sagaStateRepository.findById(sagaState.sagaId).get()
        assertEquals(SagaStatus.COMPLETED, updatedState.status)
        assertNotNull(updatedState.completedAt)
    }

    @Test
    fun `PaymentFailedEvent 처리 시 보상 트랜잭션이 시작된다`() {
        // given
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = "customer-1",
            productId = "product-1",
            quantity = 2,
            totalAmount = BigDecimal("10000")
        )

        orderSagaOrchestrator.handleEvent(
            OrderCreatedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                customerId = sagaState.customerId,
                productId = sagaState.productId,
                quantity = sagaState.quantity,
                totalAmount = sagaState.totalAmount
            )
        )
        outboxRepository.deleteAll()

        val event = PaymentFailedEvent(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            reason = "잔액 부족"
        )

        // when
        orderSagaOrchestrator.handleEvent(event)

        // then
        val updatedState = sagaStateRepository.findById(sagaState.sagaId).get()
        assertEquals(SagaStatus.COMPENSATING, updatedState.status)
        assertEquals("잔액 부족", updatedState.failureReason)

        // CancelOrder 커맨드가 발행되었는지 확인
        val pendingOutbox = outboxRepository.findByStatus(OutboxStatus.PENDING)
        assertEquals(1, pendingOutbox.size)
        assertEquals(OutboxEventType.CANCEL_ORDER, pendingOutbox[0].eventType)
    }

    @Test
    fun `InventoryFailedEvent 처리 시 결제 취소 보상 트랜잭션이 시작된다`() {
        // given
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = "customer-1",
            productId = "product-1",
            quantity = 100,
            totalAmount = BigDecimal("500000")
        )

        orderSagaOrchestrator.handleEvent(
            OrderCreatedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                customerId = sagaState.customerId,
                productId = sagaState.productId,
                quantity = sagaState.quantity,
                totalAmount = sagaState.totalAmount
            )
        )

        val paymentId = UUID.randomUUID().toString()
        orderSagaOrchestrator.handleEvent(
            PaymentCompletedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                paymentId = paymentId,
                amount = sagaState.totalAmount
            )
        )
        outboxRepository.deleteAll()

        val event = InventoryFailedEvent(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            productId = sagaState.productId,
            requestedQuantity = sagaState.quantity,
            availableQuantity = 10,
            reason = "재고 부족"
        )

        // when
        orderSagaOrchestrator.handleEvent(event)

        // then
        val updatedState = sagaStateRepository.findById(sagaState.sagaId).get()
        assertEquals(SagaStatus.COMPENSATING, updatedState.status)
        assertEquals("재고 부족", updatedState.failureReason)
        assertNotNull(updatedState.paymentId)

        // CancelPayment 커맨드가 발행되었는지 확인 (결제가 완료된 상태였으므로)
        val pendingOutbox = outboxRepository.findByStatus(OutboxStatus.PENDING)
        assertEquals(1, pendingOutbox.size)
        assertEquals(OutboxEventType.CANCEL_PAYMENT, pendingOutbox[0].eventType)
    }

    @Test
    fun `OrderCancelledEvent 처리 시 보상이 완료된다`() {
        // given
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = "customer-1",
            productId = "product-1",
            quantity = 2,
            totalAmount = BigDecimal("10000")
        )

        orderSagaOrchestrator.handleEvent(
            OrderCreatedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                customerId = sagaState.customerId,
                productId = sagaState.productId,
                quantity = sagaState.quantity,
                totalAmount = sagaState.totalAmount
            )
        )

        // 결제 실패로 보상 시작
        orderSagaOrchestrator.handleEvent(
            PaymentFailedEvent(
                sagaId = sagaState.sagaId,
                orderId = sagaState.orderId,
                reason = "결제 실패"
            )
        )

        val event = OrderCancelledEvent(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            reason = "결제 실패"
        )

        // when
        orderSagaOrchestrator.handleEvent(event)

        // then
        val updatedState = sagaStateRepository.findById(sagaState.sagaId).get()
        assertEquals(SagaStatus.COMPENSATED, updatedState.status)
        assertNotNull(updatedState.completedAt)
    }

    @Test
    fun `존재하지 않는 Saga에 대한 이벤트는 무시된다`() {
        // given
        val event = OrderCreatedEvent(
            sagaId = "non-existent-saga",
            orderId = "non-existent-order",
            customerId = "customer-1",
            productId = "product-1",
            quantity = 1,
            totalAmount = BigDecimal("1000")
        )

        // when & then - 예외 없이 처리되어야 함
        orderSagaOrchestrator.handleEvent(event)

        // Saga가 생성되지 않았는지 확인
        val state = sagaStateRepository.findById("non-existent-saga").orElse(null)
        assertNull(state)
    }
}
