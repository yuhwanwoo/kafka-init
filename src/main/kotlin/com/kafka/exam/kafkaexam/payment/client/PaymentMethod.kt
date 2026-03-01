package com.kafka.exam.kafkaexam.payment.client

enum class PaymentMethod(val description: String) {
    CARD("신용/체크카드"),
    BANK_TRANSFER("계좌이체"),
    VIRTUAL_ACCOUNT("가상계좌"),
    MOBILE("휴대폰 결제"),
    KAKAO_PAY("카카오페이"),
    NAVER_PAY("네이버페이"),
    TOSS_PAY("토스페이")
}