package com.kafka.exam.kafkaexam.order.domain

enum class OrderStatus {
    PENDING,    // 주문 생성됨, 결제 대기
    CONFIRMED,  // 결제 및 재고 확보 완료
    CANCELLED,  // 취소됨
    FAILED      // 처리 실패
}
