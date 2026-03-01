package com.kafka.exam.kafkaexam.payment.client

import java.math.BigDecimal
import java.time.LocalDateTime

// 결제 요청
data class PaymentRequest(
    val orderId: String,
    val amount: BigDecimal,
    val method: PaymentMethod = PaymentMethod.CARD,
    val customerName: String? = null,
    val customerEmail: String? = null,
    val orderName: String? = null,
    val metadata: Map<String, String> = emptyMap()
)

// 결제 응답
data class PaymentResponse(
    val success: Boolean,
    val transactionId: String?,
    val paymentKey: String?,
    val status: PaymentResponseStatus,
    val method: PaymentMethod?,
    val approvedAt: LocalDateTime?,
    val errorCode: String? = null,
    val errorMessage: String? = null
)

enum class PaymentResponseStatus {
    APPROVED,       // 승인됨
    PENDING,        // 대기 중 (가상계좌 등)
    FAILED,         // 실패
    CANCELLED,      // 취소됨
    EXPIRED         // 만료됨
}

// 환불 요청
data class RefundRequest(
    val paymentKey: String,
    val transactionId: String,
    val amount: BigDecimal,
    val reason: String,
    val refundAccount: RefundAccount? = null  // 가상계좌 환불 시 필요
)

data class RefundAccount(
    val bank: String,
    val accountNumber: String,
    val holderName: String
)

// 환불 응답
data class RefundResponse(
    val success: Boolean,
    val refundId: String?,
    val transactionId: String?,
    val refundedAmount: BigDecimal?,
    val status: RefundResponseStatus,
    val refundedAt: LocalDateTime?,
    val errorCode: String? = null,
    val errorMessage: String? = null
)

enum class RefundResponseStatus {
    COMPLETED,      // 환불 완료
    PENDING,        // 환불 처리 중
    FAILED,         // 환불 실패
    PARTIAL         // 부분 환불
}

// 결제 조회 응답
data class PaymentInquiryResponse(
    val success: Boolean,
    val transactionId: String?,
    val paymentKey: String?,
    val orderId: String?,
    val amount: BigDecimal?,
    val status: PaymentResponseStatus,
    val method: PaymentMethod?,
    val approvedAt: LocalDateTime?,
    val cancelledAt: LocalDateTime?,
    val errorCode: String? = null,
    val errorMessage: String? = null
)
