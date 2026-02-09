package com.kafka.exam.kafkaexam.saga.core

enum class SagaStatus {
    STARTED,        // Saga 시작됨
    ORDER_PENDING,  // 주문 생성 대기
    ORDER_CREATED,  // 주문 생성 완료
    PAYMENT_PENDING, // 결제 대기
    PAYMENT_COMPLETED, // 결제 완료
    INVENTORY_PENDING, // 재고 예약 대기
    COMPLETED,      // 모든 스텝 완료
    COMPENSATING,   // 보상 트랜잭션 진행 중
    COMPENSATED,    // 보상 완료
    FAILED          // 최종 실패
}
