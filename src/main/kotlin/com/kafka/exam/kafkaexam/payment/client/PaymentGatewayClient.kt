package com.kafka.exam.kafkaexam.payment.client

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.kotlin.circuitbreaker.executeSuspendFunction
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal

data class PaymentResult(
    val success: Boolean,
    val transactionId: String?,
    val errorMessage: String? = null
)

data class RefundResult(
    val success: Boolean,
    val refundId: String?,
    val errorMessage: String? = null
)

@Component
class PaymentGatewayClient(
    @Qualifier("paymentCircuitBreaker")
    private val circuitBreaker: CircuitBreaker
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun processPayment(orderId: String, amount: BigDecimal): PaymentResult {
        return try {
            runBlocking {
                circuitBreaker.executeSuspendFunction {
                    doProcessPayment(orderId, amount)
                }
            }
        } catch (e: CallNotPermittedException) {
            log.error("Circuit Breaker OPEN - 결제 처리 불가: orderId={}", orderId)
            PaymentResult(
                success = false,
                transactionId = null,
                errorMessage = "결제 서비스 일시 중단 (Circuit Breaker OPEN)"
            )
        } catch (e: Exception) {
            log.error("결제 처리 실패: orderId={}, error={}", orderId, e.message)
            PaymentResult(
                success = false,
                transactionId = null,
                errorMessage = e.message
            )
        }
    }

    fun processRefund(paymentId: String, amount: BigDecimal, reason: String): RefundResult {
        return try {
            runBlocking {
                circuitBreaker.executeSuspendFunction {
                    doProcessRefund(paymentId, amount, reason)
                }
            }
        } catch (e: CallNotPermittedException) {
            log.error("Circuit Breaker OPEN - 환불 처리 불가: paymentId={}", paymentId)
            RefundResult(
                success = false,
                refundId = null,
                errorMessage = "결제 서비스 일시 중단 (Circuit Breaker OPEN)"
            )
        } catch (e: Exception) {
            log.error("환불 처리 실패: paymentId={}, error={}", paymentId, e.message)
            RefundResult(
                success = false,
                refundId = null,
                errorMessage = e.message
            )
        }
    }

    private suspend fun doProcessPayment(orderId: String, amount: BigDecimal): PaymentResult {
        log.info("외부 결제 게이트웨이 호출: orderId={}, amount={}", orderId, amount)

        // 실제 구현에서는 외부 API 호출
        // 시뮬레이션: 랜덤하게 실패 발생 (테스트용)
        simulateExternalCall()

        val transactionId = "TXN-${System.currentTimeMillis()}"
        log.info("결제 성공: transactionId={}", transactionId)

        return PaymentResult(
            success = true,
            transactionId = transactionId
        )
    }

    private suspend fun doProcessRefund(paymentId: String, amount: BigDecimal, reason: String): RefundResult {
        log.info("외부 환불 게이트웨이 호출: paymentId={}, amount={}, reason={}", paymentId, amount, reason)

        // 실제 구현에서는 외부 API 호출
        simulateExternalCall()

        val refundId = "REF-${System.currentTimeMillis()}"
        log.info("환불 성공: refundId={}", refundId)

        return RefundResult(
            success = true,
            refundId = refundId
        )
    }

    private fun simulateExternalCall() {
        // 외부 API 호출 시뮬레이션 (지연 + 간헐적 실패)
        Thread.sleep(100) // 100ms 지연

        // 10% 확률로 실패 시뮬레이션 (프로덕션에서는 제거)
        // if (Math.random() < 0.1) {
        //     throw RuntimeException("외부 결제 게이트웨이 오류")
        // }
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
}