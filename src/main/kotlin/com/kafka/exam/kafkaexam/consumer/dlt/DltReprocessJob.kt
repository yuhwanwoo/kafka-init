package com.kafka.exam.kafkaexam.consumer.dlt

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(
    name = ["kafka.dlt.auto-reprocess.enabled"],
    havingValue = "true",
    matchIfMissing = false
)
class DltReprocessJob(
    private val dltReprocessService: DltReprocessService,
    @Value("\${kafka.dlt.auto-reprocess.batch-size:20}") private val batchSize: Int
) {
    private val log = LoggerFactory.getLogger(DltReprocessJob::class.java)

    /**
     * 자동 재처리 스케줄러
     * - 기본 1분 간격으로 실행
     * - Circuit Breaker 적용 (Kafka 장애 시 자동 차단)
     */
    @Scheduled(fixedRateString = "\${kafka.dlt.auto-reprocess.interval:60000}")
    @CircuitBreaker(name = "dltReprocess", fallbackMethod = "reprocessFallback")
    fun autoReprocess() {
        log.debug("DLT 자동 재처리 시작")

        val result = dltReprocessService.reprocessAllPending(batchSize)

        if (result.total > 0) {
            log.info(
                "DLT 자동 재처리 완료 - 총: {}, 성공: {}, 실패: {}, 스킵: {}",
                result.total, result.success, result.failed, result.skipped
            )
        }
    }

    @Suppress("unused")
    private fun reprocessFallback(e: Exception): Unit {
        log.warn("DLT 자동 재처리 Circuit Breaker 발동 - error: {}", e.message)
    }
}