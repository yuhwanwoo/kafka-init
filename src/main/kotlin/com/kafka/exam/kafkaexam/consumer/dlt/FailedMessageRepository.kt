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

    // 에러 메시지 검색 (LIKE 검색)
    @Query("SELECT f FROM FailedMessage f WHERE f.errorMessage LIKE %:keyword% ORDER BY f.createdAt DESC")
    fun searchByErrorMessage(keyword: String, pageable: Pageable): Page<FailedMessage>

    // 복합 필터 검색
    @Query("""
        SELECT f FROM FailedMessage f
        WHERE (:status IS NULL OR f.status = :status)
        AND (:topic IS NULL OR f.originalTopic = :topic)
        AND (:startDate IS NULL OR f.createdAt >= :startDate)
        AND (:endDate IS NULL OR f.createdAt <= :endDate)
        AND (:minRetryCount IS NULL OR f.retryCount >= :minRetryCount)
        ORDER BY f.createdAt DESC
    """)
    fun searchWithFilters(
        status: FailedMessageStatus?,
        topic: String?,
        startDate: LocalDateTime?,
        endDate: LocalDateTime?,
        minRetryCount: Int?,
        pageable: Pageable
    ): Page<FailedMessage>

    // 토픽 목록 조회
    @Query("SELECT DISTINCT f.originalTopic FROM FailedMessage f ORDER BY f.originalTopic")
    fun findDistinctTopics(): List<String>

    // 재시도 횟수별 카운트
    @Query("SELECT f.retryCount, COUNT(f) FROM FailedMessage f WHERE f.status = :status GROUP BY f.retryCount ORDER BY f.retryCount")
    fun countByRetryCountAndStatus(status: FailedMessageStatus): List<Array<Any>>

    // 일별 실패 메시지 카운트
    @Query("SELECT FUNCTION('DATE', f.createdAt), COUNT(f) FROM FailedMessage f WHERE f.createdAt >= :startDate GROUP BY FUNCTION('DATE', f.createdAt) ORDER BY FUNCTION('DATE', f.createdAt)")
    fun countByDaySince(startDate: LocalDateTime): List<Array<Any>>

    // 특정 토픽의 최근 메시지
    fun findByOriginalTopicOrderByCreatedAtDesc(topic: String, pageable: Pageable): Page<FailedMessage>

    // 메시지 키로 검색
    fun findByMessageKey(messageKey: String): List<FailedMessage>

    // 특정 상태가 아닌 메시지 조회
    fun findByStatusNot(status: FailedMessageStatus, pageable: Pageable): Page<FailedMessage>

    // 오래된 메시지 삭제용
    @Modifying
    @Query("DELETE FROM FailedMessage f WHERE f.status = :status AND f.createdAt < :cutoffDate")
    fun deleteByStatusAndCreatedAtBefore(status: FailedMessageStatus, cutoffDate: LocalDateTime): Int
}