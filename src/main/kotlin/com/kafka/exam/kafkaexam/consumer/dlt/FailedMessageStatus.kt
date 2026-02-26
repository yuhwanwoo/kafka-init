package com.kafka.exam.kafkaexam.consumer.dlt

enum class FailedMessageStatus {
    PENDING,    // 처리 대기 중
    RETRIED,    // 재처리 완료
    RESOLVED,   // 수동 해결
    IGNORED     // 무시 처리
}