package com.kafka.exam.kafkaexam.saga.orchestrator

import com.kafka.exam.kafkaexam.order.service.OrderService
import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.saga.command.*
import com.kafka.exam.kafkaexam.saga.core.*
import com.kafka.exam.kafkaexam.saga.event.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper
import java.math.BigDecimal
import java.util.UUID

@Component
class OrderSagaOrchestrator(
    private val sagaStateRepository: SagaStateRepository,
    private val outboxRepository: OutboxRepository,
    private val orderService: OrderService,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val SAGA_COMMAND_TOPIC = "saga-command-topic"
    }

    @Transactional
    fun startOrderSaga(
        customerId: String,
        productId: String,
        quantity: Int,
        totalAmount: BigDecimal
    ): SagaState {
        val sagaId = UUID.randomUUID().toString()
        val orderId = UUID.randomUUID().toString()

        log.info("Starting order saga: sagaId={}, orderId={}", sagaId, orderId)

        val sagaState = SagaState(
            sagaId = sagaId,
            orderId = orderId,
            customerId = customerId,
            productId = productId,
            quantity = quantity,
            totalAmount = totalAmount,
            status = SagaStatus.ORDER_PENDING,
            currentStep = SagaStep.CREATE_ORDER
        )
        sagaStateRepository.save(sagaState)

        // 첫 번째 커맨드 발행: 주문 생성
        publishCreateOrderCommand(sagaState)

        return sagaState
    }

    @Transactional
    fun handleEvent(event: SagaEvent) {
        val sagaState = sagaStateRepository.findById(event.sagaId).orElse(null)
        if (sagaState == null) {
            log.warn("Saga not found for event: sagaId={}", event.sagaId)
            return
        }

        log.info("Handling event: sagaId={}, eventType={}, currentStep={}, status={}",
            event.sagaId, event::class.simpleName, sagaState.currentStep, sagaState.status)

        when (event) {
            is OrderCreatedEvent -> handleOrderCreated(sagaState, event)
            is OrderCancelledEvent -> handleOrderCancelled(sagaState, event)
            is PaymentCompletedEvent -> handlePaymentCompleted(sagaState, event)
            is PaymentFailedEvent -> handlePaymentFailed(sagaState, event)
            is PaymentCancelledEvent -> handlePaymentCancelled(sagaState, event)
            is InventoryReservedEvent -> handleInventoryReserved(sagaState, event)
            is InventoryFailedEvent -> handleInventoryFailed(sagaState, event)
            is InventoryReleasedEvent -> handleInventoryReleased(sagaState, event)
        }
    }

    private fun handleOrderCreated(sagaState: SagaState, event: OrderCreatedEvent) {
        sagaState.status = SagaStatus.ORDER_CREATED
        sagaState.advanceToNextStep() // -> PROCESS_PAYMENT
        sagaStateRepository.save(sagaState)

        // 다음 스텝: 결제 처리
        publishProcessPaymentCommand(sagaState)
    }

    private fun handleOrderCancelled(sagaState: SagaState, event: OrderCancelledEvent) {
        // 보상 완료
        sagaState.markCompensated()
        sagaStateRepository.save(sagaState)
        log.info("Saga compensation completed: sagaId={}", sagaState.sagaId)
    }

    private fun handlePaymentCompleted(sagaState: SagaState, event: PaymentCompletedEvent) {
        sagaState.status = SagaStatus.PAYMENT_COMPLETED
        sagaState.paymentId = event.paymentId
        sagaState.advanceToNextStep() // -> RESERVE_INVENTORY
        sagaStateRepository.save(sagaState)

        // 다음 스텝: 재고 예약
        publishReserveInventoryCommand(sagaState)
    }

    private fun handlePaymentFailed(sagaState: SagaState, event: PaymentFailedEvent) {
        log.warn("Payment failed for sagaId={}, reason={}", sagaState.sagaId, event.reason)
        startCompensation(sagaState, event.reason)
    }

    private fun handlePaymentCancelled(sagaState: SagaState, event: PaymentCancelledEvent) {
        // 결제 취소 완료, 다음 보상 스텝으로
        publishCancelOrderCommand(sagaState, event.reason)
    }

    private fun handleInventoryReserved(sagaState: SagaState, event: InventoryReservedEvent) {
        // 모든 스텝 완료
        sagaState.markCompleted()
        sagaStateRepository.save(sagaState)

        // 주문 확정
        orderService.confirmOrder(sagaState.orderId)

        log.info("Saga completed successfully: sagaId={}", sagaState.sagaId)
    }

    private fun handleInventoryFailed(sagaState: SagaState, event: InventoryFailedEvent) {
        log.warn("Inventory reservation failed for sagaId={}, reason={}", sagaState.sagaId, event.reason)
        startCompensation(sagaState, event.reason)
    }

    private fun handleInventoryReleased(sagaState: SagaState, event: InventoryReleasedEvent) {
        // 재고 해제 완료, 다음 보상 스텝으로
        if (sagaState.paymentId != null) {
            publishCancelPaymentCommand(sagaState, sagaState.failureReason ?: "Compensation")
        } else {
            publishCancelOrderCommand(sagaState, sagaState.failureReason ?: "Compensation")
        }
    }

    private fun startCompensation(sagaState: SagaState, reason: String) {
        sagaState.startCompensation(reason)
        sagaStateRepository.save(sagaState)

        // 현재 스텝에 따라 보상 시작
        when (sagaState.currentStep) {
            SagaStep.RESERVE_INVENTORY, SagaStep.COMPLETE_SAGA -> {
                // 결제까지 완료된 상태에서 재고 실패
                if (sagaState.paymentId != null) {
                    publishCancelPaymentCommand(sagaState, reason)
                } else {
                    publishCancelOrderCommand(sagaState, reason)
                }
            }
            SagaStep.PROCESS_PAYMENT -> {
                // 결제 처리 중 실패
                publishCancelOrderCommand(sagaState, reason)
            }
            else -> {
                // 주문 생성 단계에서 실패
                sagaState.markFailed(reason)
                sagaStateRepository.save(sagaState)
            }
        }
    }

    private fun publishCreateOrderCommand(sagaState: SagaState) {
        val command = CreateOrderCommand(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            customerId = sagaState.customerId,
            productId = sagaState.productId,
            quantity = sagaState.quantity,
            totalAmount = sagaState.totalAmount
        )
        publishCommand(command, OutboxEventType.CREATE_ORDER)
    }

    private fun publishProcessPaymentCommand(sagaState: SagaState) {
        sagaState.status = SagaStatus.PAYMENT_PENDING
        sagaStateRepository.save(sagaState)

        val command = ProcessPaymentCommand(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            customerId = sagaState.customerId,
            amount = sagaState.totalAmount
        )
        publishCommand(command, OutboxEventType.PROCESS_PAYMENT)
    }

    private fun publishReserveInventoryCommand(sagaState: SagaState) {
        sagaState.status = SagaStatus.INVENTORY_PENDING
        sagaStateRepository.save(sagaState)

        val command = ReserveInventoryCommand(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            productId = sagaState.productId,
            quantity = sagaState.quantity
        )
        publishCommand(command, OutboxEventType.RESERVE_INVENTORY)
    }

    private fun publishCancelPaymentCommand(sagaState: SagaState, reason: String) {
        val command = CancelPaymentCommand(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            paymentId = sagaState.paymentId!!,
            reason = reason
        )
        publishCommand(command, OutboxEventType.CANCEL_PAYMENT)
    }

    private fun publishCancelOrderCommand(sagaState: SagaState, reason: String) {
        val command = CancelOrderCommand(
            sagaId = sagaState.sagaId,
            orderId = sagaState.orderId,
            reason = reason
        )
        publishCommand(command, OutboxEventType.CANCEL_ORDER)
    }

    private fun publishCommand(command: SagaCommand, eventType: OutboxEventType) {
        val outbox = Outbox(
            aggregateType = "Saga",
            aggregateId = command.sagaId,
            eventType = eventType,
            payload = objectMapper.writeValueAsString(command),
            topic = SAGA_COMMAND_TOPIC
        )
        outboxRepository.save(outbox)
        log.info("Published command: type={}, sagaId={}", eventType, command.sagaId)
    }
}
