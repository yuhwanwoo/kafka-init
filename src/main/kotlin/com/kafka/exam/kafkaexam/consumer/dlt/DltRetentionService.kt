package com.kafka.exam.kafkaexam.consumer.dlt

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class DltRetentionService(
    private val failedMessageRepository: FailedMessageRepository,
    @Value("\${kafka.dlt.retention.resolved-days:7}") private val resolvedRetentionDays: Long,
    @Value("\${kafka.dlt.retention.ignored-days:3}") private val ignoredRetentionDays: Long,
    @Value("\${kafka.dlt.retention.retried-days:30}") private val retriedRetentionDays: Long,
    @Value("\${kafka.dlt.retention.pending-days:90}") private val pendingRetentionDays: Long
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 수동 정리 - 해결된 메시지 삭제
     */
    @Transactional
    fun cleanupResolved(): CleanupResult {
        val cutoffDate = LocalDateTime.now().minusDays(resolvedRetentionDays)
        return cleanupByStatusAndDate(FailedMessageStatus.RESOLVED, cutoffDate)
    }

    /**
     * 수동 정리 - 무시된 메시지 삭제
     */
    @Transactional
    fun cleanupIgnored(): CleanupResult {
        val cutoffDate = LocalDateTime.now().minusDays(ignoredRetentionDays)
        return cleanupByStatusAndDate(FailedMessageStatus.IGNORED, cutoffDate)
    }

    /**
     * 수동 정리 - 재처리 완료 메시지 삭제
     */
    @Transactional
    fun cleanupRetried(): CleanupResult {
        val cutoffDate = LocalDateTime.now().minusDays(retriedRetentionDays)
        return cleanupByStatusAndDate(FailedMessageStatus.RETRIED, cutoffDate)
    }

    /**
     * 수동 정리 - 오래된 PENDING 메시지 아카이브 처리
     */
    @Transactional
    fun archiveOldPending(): CleanupResult {
        val cutoffDate = LocalDateTime.now().minusDays(pendingRetentionDays)
        val oldMessages = failedMessageRepository.findByCreatedAtBetweenAndStatus(
            LocalDateTime.MIN, cutoffDate, FailedMessageStatus.PENDING
        )

        if (oldMessages.isEmpty()) {
            return CleanupResult(FailedMessageStatus.PENDING, 0, "No old pending messages found")
        }

        // PENDING 메시지는 삭제하지 않고 IGNORED로 변경
        val ids = oldMessages.mapNotNull { it.id }
        val updatedCount = failedMessageRepository.bulkUpdateStatus(
            ids, FailedMessageStatus.IGNORED, LocalDateTime.now()
        )

        log.info("Archived {} old pending messages (older than {} days)", updatedCount, pendingRetentionDays)
        return CleanupResult(
            FailedMessageStatus.PENDING,
            updatedCount,
            "Archived $updatedCount messages to IGNORED status"
        )
    }

    /**
     * 전체 정리 실행
     */
    @Transactional
    fun runFullCleanup(): FullCleanupResult {
        val results = mutableListOf<CleanupResult>()

        results.add(cleanupResolved())
        results.add(cleanupIgnored())
        results.add(cleanupRetried())
        results.add(archiveOldPending())

        val totalDeleted = results.sumOf { it.deletedCount }
        log.info("Full cleanup completed - total affected: {}", totalDeleted)

        return FullCleanupResult(
            results = results,
            totalAffected = totalDeleted,
            executedAt = LocalDateTime.now()
        )
    }

    /**
     * 특정 상태의 모든 메시지 삭제 (관리자 전용)
     */
    @Transactional
    fun deleteAllByStatus(status: FailedMessageStatus): CleanupResult {
        val messages = failedMessageRepository.findByStatus(status)
        val count = messages.size

        if (count == 0) {
            return CleanupResult(status, 0, "No messages found")
        }

        failedMessageRepository.deleteAll(messages)
        log.info("Deleted all {} messages with status {}", count, status)

        return CleanupResult(status, count, "Deleted all $count messages")
    }

    /**
     * ID 목록으로 삭제
     */
    @Transactional
    fun deleteByIds(ids: List<Long>): Int {
        val messages = failedMessageRepository.findAllById(ids)
        failedMessageRepository.deleteAll(messages)
        log.info("Deleted {} messages by IDs", messages.size)
        return messages.size
    }

    /**
     * 보존 정책 정보 조회
     */
    fun getRetentionPolicy(): RetentionPolicy {
        return RetentionPolicy(
            resolvedRetentionDays = resolvedRetentionDays,
            ignoredRetentionDays = ignoredRetentionDays,
            retriedRetentionDays = retriedRetentionDays,
            pendingRetentionDays = pendingRetentionDays
        )
    }

    /**
     * 정리 예상 결과 조회 (dry-run)
     */
    fun previewCleanup(): CleanupPreview {
        val now = LocalDateTime.now()

        val resolvedToDelete = countMessagesOlderThan(
            FailedMessageStatus.RESOLVED,
            now.minusDays(resolvedRetentionDays)
        )
        val ignoredToDelete = countMessagesOlderThan(
            FailedMessageStatus.IGNORED,
            now.minusDays(ignoredRetentionDays)
        )
        val retriedToDelete = countMessagesOlderThan(
            FailedMessageStatus.RETRIED,
            now.minusDays(retriedRetentionDays)
        )
        val pendingToArchive = countMessagesOlderThan(
            FailedMessageStatus.PENDING,
            now.minusDays(pendingRetentionDays)
        )

        return CleanupPreview(
            resolvedToDelete = resolvedToDelete,
            ignoredToDelete = ignoredToDelete,
            retriedToDelete = retriedToDelete,
            pendingToArchive = pendingToArchive,
            totalToAffect = resolvedToDelete + ignoredToDelete + retriedToDelete + pendingToArchive,
            policy = getRetentionPolicy()
        )
    }

    private fun cleanupByStatusAndDate(status: FailedMessageStatus, cutoffDate: LocalDateTime): CleanupResult {
        val messages = failedMessageRepository.findByCreatedAtBetweenAndStatus(
            LocalDateTime.MIN, cutoffDate, status
        )

        if (messages.isEmpty()) {
            return CleanupResult(status, 0, "No messages to cleanup")
        }

        failedMessageRepository.deleteAll(messages)
        log.info("Cleaned up {} messages with status {} older than {}", messages.size, status, cutoffDate)

        return CleanupResult(status, messages.size, "Deleted ${messages.size} messages")
    }

    private fun countMessagesOlderThan(status: FailedMessageStatus, cutoffDate: LocalDateTime): Int {
        return failedMessageRepository.findByCreatedAtBetweenAndStatus(
            LocalDateTime.MIN, cutoffDate, status
        ).size
    }
}

/**
 * 자동 정리 스케줄러
 */
@Service
@ConditionalOnProperty(
    name = ["kafka.dlt.retention.auto-cleanup.enabled"],
    havingValue = "true",
    matchIfMissing = false
)
class DltRetentionJob(
    private val retentionService: DltRetentionService
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 매일 새벽 3시에 자동 정리 실행
     */
    @Scheduled(cron = "\${kafka.dlt.retention.auto-cleanup.cron:0 0 3 * * *}")
    fun scheduledCleanup() {
        log.info("Starting scheduled DLT cleanup")
        val result = retentionService.runFullCleanup()
        log.info("Scheduled cleanup completed - total affected: {}", result.totalAffected)
    }
}

// DTOs

data class CleanupResult(
    val status: FailedMessageStatus,
    val deletedCount: Int,
    val message: String
)

data class FullCleanupResult(
    val results: List<CleanupResult>,
    val totalAffected: Int,
    val executedAt: LocalDateTime
)

data class RetentionPolicy(
    val resolvedRetentionDays: Long,
    val ignoredRetentionDays: Long,
    val retriedRetentionDays: Long,
    val pendingRetentionDays: Long
)

data class CleanupPreview(
    val resolvedToDelete: Int,
    val ignoredToDelete: Int,
    val retriedToDelete: Int,
    val pendingToArchive: Int,
    val totalToAffect: Int,
    val policy: RetentionPolicy
)
