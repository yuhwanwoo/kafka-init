package com.kafka.exam.kafkaexam.payment.service

import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.payment.client.*
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
    private val objectMapper: ObjectMapper,
    private val paymentGatewayClient: PaymentGatewayClient
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val SAGA_EVENT_TOPIC = "saga-event-topic"
    }

    @Transactional
    fun processPayment(command: ProcessPaymentCommand, method: PaymentMethod = PaymentMethod.CARD): Payment {
        log.info("Processing payment for sagaId={}, orderId={}, amount={}, method={}",
            command.sagaId, command.orderId, command.amount, method)

        val payment = Payment(
            sagaId = command.sagaId,
            orderId = command.orderId,
            customerId = command.customerId,
            amount = command.amount
        )

        val request = PaymentRequest(
            orderId = command.orderId,
            amount = command.amount,
            method = method,
            customerName = command.customerId,
            orderName = "Order-${command.orderId}"
        )

        val result = paymentGatewayClient.processPayment(request)

        if (result.success) {
            payment.complete(result.transactionId, result.paymentKey)
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

            log.info("Payment completed: paymentId={}, transactionId={}, paymentKey={}",
                payment.paymentId, result.transactionId, result.paymentKey)
        } else {
            payment.fail(result.errorMessage ?: "결제 실패")
            paymentRepository.save(payment)

            val event = PaymentFailedEvent(
                sagaId = command.sagaId,
                orderId = command.orderId,
                reason = result.errorMessage ?: "결제 실패"
            )

            val outbox = Outbox(
                aggregateType = "Payment",
                aggregateId = payment.paymentId,
                eventType = OutboxEventType.PAYMENT_FAILED,
                payload = objectMapper.writeValueAsString(event),
                topic = SAGA_EVENT_TOPIC
            )
            outboxRepository.save(outbox)

            log.error("Payment failed: paymentId={}, errorCode={}, reason={}",
                payment.paymentId, result.errorCode, result.errorMessage)
        }

        return payment
    }

    @Transactional
    fun cancelPayment(command: CancelPaymentCommand) {
        log.info("Cancelling payment for sagaId={}, paymentId={}",
            command.sagaId, command.paymentId)

        val payment = paymentRepository.findById(command.paymentId)
            .orElseThrow { IllegalArgumentException("Payment not found: ${command.paymentId}") }

        // 외부 결제 게이트웨이 환불 호출
        val refundRequest = RefundRequest(
            paymentKey = payment.paymentKey ?: "",
            transactionId = payment.transactionId ?: "",
            amount = payment.amount,
            reason = command.reason
        )

        val refundResult = paymentGatewayClient.processRefund(refundRequest)

        if (refundResult.success) {
            payment.cancel(command.reason, refundResult.refundId)
            log.info("Refund completed: paymentId={}, refundId={}", payment.paymentId, refundResult.refundId)
        } else {
            // 환불 실패 시에도 취소 상태로 변경 (수동 처리 필요)
            payment.cancel("${command.reason} (환불 실패: ${refundResult.errorMessage})", null)
            log.error("Refund failed but payment cancelled: paymentId={}, errorCode={}, reason={}",
                payment.paymentId, refundResult.errorCode, refundResult.errorMessage)
        }

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

    fun inquirePayment(paymentKey: String): PaymentInquiryResponse {
        return paymentGatewayClient.inquirePayment(paymentKey)
    }
}
