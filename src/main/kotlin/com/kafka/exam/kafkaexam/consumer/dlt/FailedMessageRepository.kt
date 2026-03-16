package com.kafka.exam.kafkaexam.consumer.dlt

import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.time.LocalDateTime

interface FailedMessageRepository : JpaRepository<FailedMessage, Long> {

    fun findByStatus(status: FailedMessageStatus): List<FailedMessage>

    fun findByOriginalTopic(topic: String): List<FailedMessage>

    fun findByOriginalTopicAndStatus(topic: String, status: FailedMessageStatus): List<FailedMessage>

    fun countByStatus(status: FailedMessageStatus): Long

    // 페이징 조회
    fun findByStatus(status: FailedMessageStatus, pageable: Pageable): Page<FailedMessage>

    fun findByOriginalTopicAndStatus(topic: String, status: FailedMessageStatus, pageable: Pageable): Page<FailedMessage>

    // 재처리 대상 조회 (PENDING 상태 + 재시도 횟수 제한)
    @Query("SELECT f FROM FailedMessage f WHERE f.status = :status AND f.retryCount < :maxRetryCount ORDER BY f.createdAt ASC")
    fun findReprocessableMessages(status: FailedMessageStatus, maxRetryCount: Int, pageable: Pageable): Page<FailedMessage>

    // 특정 기간 내 실패 메시지 조회
    @Query("SELECT f FROM FailedMessage f WHERE f.createdAt BETWEEN :startDate AND :endDate AND f.status = :status")
    fun findByCreatedAtBetweenAndStatus(
        startDate: LocalDateTime,
        endDate: LocalDateTime,
        status: FailedMessageStatus
    ): List<FailedMessage>

    // 통계 조회
    @Query("SELECT f.originalTopic, f.status, COUNT(f) FROM FailedMessage f GROUP BY f.originalTopic, f.status")
    fun getStatisticsByTopicAndStatus(): List<Array<Any>>

    // 벌크 상태 업데이트
    @Modifying
    @Query("UPDATE FailedMessage f SET f.status = :newStatus, f.resolvedAt = :resolvedAt WHERE f.id IN :ids")
    fun bulkUpdateStatus(ids: List<Long>, newStatus: FailedMessageStatus, resolvedAt: LocalDateTime): Int
}