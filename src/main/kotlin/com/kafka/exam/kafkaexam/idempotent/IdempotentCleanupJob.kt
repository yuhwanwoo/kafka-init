package com.kafka.exam.kafkaexam.idempotent

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class IdempotentCleanupJob(
    private val idempotentConsumerService: IdempotentConsumerService
) {

    private val log = LoggerFactory.getLogger(IdempotentCleanupJob::class.java)

    // 매일 새벽 3시에 만료된 처리 기록 정리
    @Scheduled(cron = "0 0 3 * * *")
    fun cleanup() {
        log.info("[멱등성] 만료 기록 정리 시작")
        val deleted = idempotentConsumerService.cleanupExpired()
        log.info("[멱등성] 만료 기록 정리 완료 - {}건 삭제", deleted)
    }
}