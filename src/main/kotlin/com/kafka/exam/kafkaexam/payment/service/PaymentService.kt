package com.kafka.exam.kafkaexam.payment.service

import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.payment.domain.Payment
import com.kafka.exam.kafkaexam.payment.domain.PaymentRepository
import com.kafka.exam.kafkaexam.saga.command.CancelPaymentCommand
import com.kafka.exam.kafkaexam.saga.command.ProcessPaymentCommand
import com.kafka.exam.kafkaexam.saga.event.PaymentCancelledEvent
import com.kafka.exam.kafkaexam.saga.event.PaymentCompletedEvent
import com.kafka.exam.kafkaexam.saga.event.PaymentFailedEvent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class PaymentService(
    private val paymentRepository: PaymentRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val SAGA_EVENT_TOPIC = "saga-event-topic"
    }

    @Transactional
    fun processPayment(command: ProcessPaymentCommand): Payment {
        log.info("Processing payment for sagaId={}, orderId={}, amount={}",
            command.sagaId, command.orderId, command.amount)

        val payment = Payment(
            sagaId = command.sagaId,
            orderId = command.orderId,
            customerId = command.customerId,
            amount = command.amount
        )

        // 실제로는 여기서 결제 게이트웨이 호출
        // 간단히 항상 성공으로 처리
        payment.complete()
        paymentRepository.save(payment)

        val event = PaymentCompletedEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            paymentId = payment.paymentId,
            amount = command.amount
        )

        val outbox = Outbox(
            aggregateType = "Payment",
            aggregateId = payment.paymentId,
            eventType = OutboxEventType.PAYMENT_COMPLETED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)

        log.info("Payment completed: paymentId={}", payment.paymentId)
        return payment
    }

    @Transactional
    fun cancelPayment(command: CancelPaymentCommand) {
        log.info("Cancelling payment for sagaId={}, paymentId={}",
            command.sagaId, command.paymentId)

        val payment = paymentRepository.findById(command.paymentId)
            .orElseThrow { IllegalArgumentException("Payment not found: ${command.paymentId}") }

        // 실제로는 여기서 환불 처리
        payment.cancel(command.reason)

        val event = PaymentCancelledEvent(
            sagaId = command.sagaId,
            orderId = command.orderId,
            paymentId = payment.paymentId,
            reason = command.reason
        )

        val outbox = Outbox(
            aggregateType = "Payment",
            aggregateId = payment.paymentId,
            eventType = OutboxEventType.PAYMENT_CANCELLED,
            payload = objectMapper.writeValueAsString(event),
            topic = SAGA_EVENT_TOPIC
        )
        outboxRepository.save(outbox)

        log.info("Payment cancelled: paymentId={}", payment.paymentId)
    }

    fun findByPaymentId(paymentId: String): Payment? {
        return paymentRepository.findById(paymentId).orElse(null)
    }

    fun findBySagaId(sagaId: String): Payment? {
        return paymentRepository.findBySagaId(sagaId)
    }
}
