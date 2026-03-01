package com.kafka.exam.kafkaexam.payment.client

import com.kafka.exam.kafkaexam.payment.config.PaymentGatewayConfig
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.kotlin.circuitbreaker.executeSuspendFunction
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Component
class PaymentGatewayClient(
    @Qualifier("paymentCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
    private val config: PaymentGatewayConfig
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun processPayment(request: PaymentRequest): PaymentResponse {
        return try {
            runBlocking {
                circuitBreaker.executeSuspendFunction {
                    if (config.simulationMode) {
                        simulatePayment(request)
                    } else {
                        callExternalPaymentApi(request)
                    }
                }
            }
        } catch (e: CallNotPermittedException) {
            log.error("Circuit Breaker OPEN - 결제 처리 불가: orderId={}", request.orderId)
            PaymentResponse(
                success = false,
                transactionId = null,
                paymentKey = null,
                status = PaymentResponseStatus.FAILED,
                method = request.method,
                approvedAt = null,
                errorCode = "CIRCUIT_BREAKER_OPEN",
                errorMessage = "결제 서비스 일시 중단 (Circuit Breaker OPEN)"
            )
        } catch (e: Exception) {
            log.error("결제 처리 실패: orderId={}, error={}", request.orderId, e.message)
            PaymentResponse(
                success = false,
                transactionId = null,
                paymentKey = null,
                status = PaymentResponseStatus.FAILED,
                method = request.method,
                approvedAt = null,
                errorCode = "PAYMENT_ERROR",
                errorMessage = e.message
            )
        }
    }

    fun processRefund(request: RefundRequest): RefundResponse {
        return try {
            runBlocking {
                circuitBreaker.executeSuspendFunction {
                    if (config.simulationMode) {
                        simulateRefund(request)
                    } else {
                        callExternalRefundApi(request)
                    }
                }
            }
        } catch (e: CallNotPermittedException) {
            log.error("Circuit Breaker OPEN - 환불 처리 불가: transactionId={}", request.transactionId)
            RefundResponse(
                success = false,
                refundId = null,
                transactionId = request.transactionId,
                refundedAmount = null,
                status = RefundResponseStatus.FAILED,
                refundedAt = null,
                errorCode = "CIRCUIT_BREAKER_OPEN",
                errorMessage = "결제 서비스 일시 중단 (Circuit Breaker OPEN)"
            )
        } catch (e: Exception) {
            log.error("환불 처리 실패: transactionId={}, error={}", request.transactionId, e.message)
            RefundResponse(
                success = false,
                refundId = null,
                transactionId = request.transactionId,
                refundedAmount = null,
                status = RefundResponseStatus.FAILED,
                refundedAt = null,
                errorCode = "REFUND_ERROR",
                errorMessage = e.message
            )
        }
    }

    fun inquirePayment(paymentKey: String): PaymentInquiryResponse {
        return try {
            runBlocking {
                circuitBreaker.executeSuspendFunction {
                    if (config.simulationMode) {
                        simulateInquiry(paymentKey)
                    } else {
                        callExternalInquiryApi(paymentKey)
                    }
                }
            }
        } catch (e: Exception) {
            log.error("결제 조회 실패: paymentKey={}, error={}", paymentKey, e.message)
            PaymentInquiryResponse(
                success = false,
                transactionId = null,
                paymentKey = paymentKey,
                orderId = null,
                amount = null,
                status = PaymentResponseStatus.FAILED,
                method = null,
                approvedAt = null,
                cancelledAt = null,
                errorCode = "INQUIRY_ERROR",
                errorMessage = e.message
            )
        }
    }

    // 시뮬레이션 모드 구현
    private suspend fun simulatePayment(request: PaymentRequest): PaymentResponse {
        log.info("[시뮬레이션] 결제 처리: orderId={}, amount={}, method={}",
            request.orderId, request.amount, request.method)

        simulateDelay()
        simulateFailure()

        val transactionId = "TXN-${System.currentTimeMillis()}-${UUID.randomUUID().toString().take(8)}"
        val paymentKey = "PK-${UUID.randomUUID()}"

        log.info("[시뮬레이션] 결제 성공: transactionId={}, paymentKey={}", transactionId, paymentKey)

        return PaymentResponse(
            success = true,
            transactionId = transactionId,
            paymentKey = paymentKey,
            status = PaymentResponseStatus.APPROVED,
            method = request.method,
            approvedAt = LocalDateTime.now()
        )
    }

    private suspend fun simulateRefund(request: RefundRequest): RefundResponse {
        log.info("[시뮬레이션] 환불 처리: transactionId={}, amount={}, reason={}",
            request.transactionId, request.amount, request.reason)

        simulateDelay()
        simulateFailure()

        val refundId = "REF-${System.currentTimeMillis()}-${UUID.randomUUID().toString().take(8)}"

        log.info("[시뮬레이션] 환불 성공: refundId={}", refundId)

        return RefundResponse(
            success = true,
            refundId = refundId,
            transactionId = request.transactionId,
            refundedAmount = request.amount,
            status = RefundResponseStatus.COMPLETED,
            refundedAt = LocalDateTime.now()
        )
    }

    private suspend fun simulateInquiry(paymentKey: String): PaymentInquiryResponse {
        log.info("[시뮬레이션] 결제 조회: paymentKey={}", paymentKey)

        simulateDelay()

        return PaymentInquiryResponse(
            success = true,
            transactionId = "TXN-SIMULATED",
            paymentKey = paymentKey,
            orderId = "ORDER-SIMULATED",
            amount = BigDecimal("10000"),
            status = PaymentResponseStatus.APPROVED,
            method = PaymentMethod.CARD,
            approvedAt = LocalDateTime.now().minusMinutes(5),
            cancelledAt = null
        )
    }

    private fun simulateDelay() {
        val delay = if (Math.random() < config.simulation.slowCallRate) {
            config.simulation.slowCallDelayMs
        } else {
            config.simulation.delayMs
        }
        Thread.sleep(delay)
    }

    private fun simulateFailure() {
        if (Math.random() < config.simulation.failureRate) {
            throw RuntimeException("시뮬레이션 결제 실패")
        }
    }

    // 실제 API 호출 (추후 구현)
    private suspend fun callExternalPaymentApi(request: PaymentRequest): PaymentResponse {
        log.info("외부 결제 API 호출: baseUrl={}, orderId={}", config.baseUrl, request.orderId)

        // TODO: 실제 PG사 API 연동 시 WebClient 사용
        // val response = webClient.post()
        //     .uri("${config.baseUrl}/v1/payments")
        //     .header("Authorization", "Basic ${encodeCredentials()}")
        //     .bodyValue(request)
        //     .retrieve()
        //     .bodyToMono(PaymentApiResponse::class.java)
        //     .awaitSingle()

        throw NotImplementedError("실제 API 연동이 필요합니다. payment.gateway.simulation-mode=true로 설정하세요.")
    }

    private suspend fun callExternalRefundApi(request: RefundRequest): RefundResponse {
        log.info("외부 환불 API 호출: baseUrl={}, transactionId={}", config.baseUrl, request.transactionId)

        // TODO: 실제 PG사 API 연동 시 WebClient 사용
        throw NotImplementedError("실제 API 연동이 필요합니다. payment.gateway.simulation-mode=true로 설정하세요.")
    }

    private suspend fun callExternalInquiryApi(paymentKey: String): PaymentInquiryResponse {
        log.info("외부 결제 조회 API 호출: baseUrl={}, paymentKey={}", config.baseUrl, paymentKey)

        // TODO: 실제 PG사 API 연동 시 WebClient 사용
        throw NotImplementedError("실제 API 연동이 필요합니다. payment.gateway.simulation-mode=true로 설정하세요.")
    }

    fun getCircuitBreakerState(): String {
        return circuitBreaker.state.name
    }

    fun getCircuitBreakerMetrics(): Map<String, Any> {
        val metrics = circuitBreaker.metrics
        return mapOf(
            "state" to circuitBreaker.state.name,
            "failureRate" to metrics.failureRate,
            "slowCallRate" to metrics.slowCallRate,
            "numberOfBufferedCalls" to metrics.numberOfBufferedCalls,
            "numberOfFailedCalls" to metrics.numberOfFailedCalls,
            "numberOfSuccessfulCalls" to metrics.numberOfSuccessfulCalls,
            "numberOfSlowCalls" to metrics.numberOfSlowCalls,
            "numberOfNotPermittedCalls" to metrics.numberOfNotPermittedCalls
        )
    }

    fun isSimulationMode(): Boolean = config.simulationMode
}