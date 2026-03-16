package com.kafka.exam.kafkaexam.consumer.dlt

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.Page
import org.springframework.data.domain.PageRequest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class DltReprocessService(
    private val failedMessageRepository: FailedMessageRepository,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    @Value("\${kafka.dlt.max-retry-count:5}") private val maxRetryCount: Int
) {
    private val log = LoggerFactory.getLogger(DltReprocessService::class.java)

    /**
     * 단건 재처리 - 원본 토픽으로 메시지 재전송
     */
    @Transactional
    fun reprocess(id: Long): ReprocessResult {
        val failedMessage = failedMessageRepository.findById(id).orElse(null)
            ?: return ReprocessResult.notFound(id)

        if (failedMessage.status != FailedMessageStatus.PENDING) {
            return ReprocessResult.invalidStatus(id, failedMessage.status)
        }

        if (failedMessage.retryCount >= maxRetryCount) {
            return ReprocessResult.maxRetryExceeded(id, failedMessage.retryCount, maxRetryCount)
        }

        return try {
            sendToOriginalTopic(failedMessage)

            failedMessage.status = FailedMessageStatus.RETRIED
            failedMessage.resolvedAt = LocalDateTime.now()
            failedMessage.retryCount++
            failedMessageRepository.save(failedMessage)

            log.info("메시지 재처리 성공 - id: {}, topic: {}", id, failedMessage.originalTopic)
            ReprocessResult.success(id)
        } catch (e: Exception) {
            failedMessage.retryCount++
            failedMessageRepository.save(failedMessage)

            log.error("메시지 재처리 실패 - id: {}, error: {}", id, e.message)
            ReprocessResult.failed(id, e.message ?: "Unknown error")
        }
    }

    /**
     * 다건 재처리
     */
    @Transactional
    fun reprocessBatch(ids: List<Long>): BatchReprocessResult {
        val results = ids.map { reprocess(it) }
        return BatchReprocessResult(
            total = ids.size,
            success = results.count { it.status == ReprocessStatus.SUCCESS },
            failed = results.count { it.status == ReprocessStatus.FAILED },
            skipped = results.count { it.status != ReprocessStatus.SUCCESS && it.status != ReprocessStatus.FAILED },
            details = results
        )
    }

    /**
     * 토픽별 PENDING 상태 메시지 일괄 재처리
     */
    @Transactional
    fun reprocessByTopic(topic: String, limit: Int = 100): BatchReprocessResult {
        val messages = failedMessageRepository.findByOriginalTopicAndStatus(
            topic,
            FailedMessageStatus.PENDING,
            PageRequest.of(0, limit)
        )
        return reprocessBatch(messages.content.mapNotNull { it.id })
    }

    /**
     * 모든 PENDING 메시지 재처리 (자동 재처리용)
     */
    @Transactional
    fun reprocessAllPending(batchSize: Int = 50): BatchReprocessResult {
        val messages = failedMessageRepository.findReprocessableMessages(
            FailedMessageStatus.PENDING,
            maxRetryCount,
            PageRequest.of(0, batchSize)
        )

        if (messages.isEmpty) {
            log.debug("재처리 대상 메시지 없음")
            return BatchReprocessResult.empty()
        }

        log.info("재처리 시작 - {} 건", messages.totalElements)
        return reprocessBatch(messages.content.mapNotNull { it.id })
    }

    /**
     * 상태 변경 - 무시 처리
     */
    @Transactional
    fun ignore(id: Long): Boolean {
        return updateStatus(id, FailedMessageStatus.IGNORED)
    }

    /**
     * 상태 변경 - 수동 해결 처리
     */
    @Transactional
    fun resolve(id: Long): Boolean {
        return updateStatus(id, FailedMessageStatus.RESOLVED)
    }

    /**
     * 벌크 상태 변경
     */
    @Transactional
    fun bulkUpdateStatus(ids: List<Long>, status: FailedMessageStatus): Int {
        return failedMessageRepository.bulkUpdateStatus(ids, status, LocalDateTime.now())
    }

    /**
     * 실패 메시지 조회 (페이징)
     */
    fun getFailedMessages(
        status: FailedMessageStatus? = null,
        topic: String? = null,
        page: Int = 0,
        size: Int = 20
    ): Page<FailedMessage> {
        val pageable = PageRequest.of(page, size)

        return when {
            topic != null && status != null ->
                failedMessageRepository.findByOriginalTopicAndStatus(topic, status, pageable)
            status != null ->
                failedMessageRepository.findByStatus(status, pageable)
            else ->
                failedMessageRepository.findAll(pageable)
        }
    }

    /**
     * 통계 조회
     */
    fun getStatistics(): DltStatistics {
        val stats = failedMessageRepository.getStatisticsByTopicAndStatus()
        val byTopic = mutableMapOf<String, MutableMap<FailedMessageStatus, Long>>()

        stats.forEach { row ->
            val topic = row[0] as String
            val status = row[1] as FailedMessageStatus
            val count = row[2] as Long

            byTopic.getOrPut(topic) { mutableMapOf() }[status] = count
        }

        return DltStatistics(
            pending = failedMessageRepository.countByStatus(FailedMessageStatus.PENDING),
            retried = failedMessageRepository.countByStatus(FailedMessageStatus.RETRIED),
            resolved = failedMessageRepository.countByStatus(FailedMessageStatus.RESOLVED),
            ignored = failedMessageRepository.countByStatus(FailedMessageStatus.IGNORED),
            byTopic = byTopic
        )
    }

    private fun sendToOriginalTopic(failedMessage: FailedMessage) {
        val topic = failedMessage.originalTopic
        val key = failedMessage.messageKey
        val value = failedMessage.messageValue ?: throw IllegalStateException("메시지 값이 없습니다")

        if (key != null) {
            kafkaTemplate.send(topic, key, value).get()
        } else {
            kafkaTemplate.send(topic, value).get()
        }
    }

    private fun updateStatus(id: Long, newStatus: FailedMessageStatus): Boolean {
        val failedMessage = failedMessageRepository.findById(id).orElse(null) ?: return false
        failedMessage.status = newStatus
        failedMessage.resolvedAt = LocalDateTime.now()
        failedMessageRepository.save(failedMessage)
        log.info("메시지 상태 변경 - id: {}, status: {}", id, newStatus)
        return true
    }
}

// Result DTOs
enum class ReprocessStatus {
    SUCCESS, FAILED, NOT_FOUND, INVALID_STATUS, MAX_RETRY_EXCEEDED
}

data class ReprocessResult(
    val id: Long,
    val status: ReprocessStatus,
    val message: String
) {
    companion object {
        fun success(id: Long) = ReprocessResult(id, ReprocessStatus.SUCCESS, "재처리 성공")
        fun failed(id: Long, error: String) = ReprocessResult(id, ReprocessStatus.FAILED, "재처리 실패: $error")
        fun notFound(id: Long) = ReprocessResult(id, ReprocessStatus.NOT_FOUND, "메시지를 찾을 수 없습니다")
        fun invalidStatus(id: Long, status: FailedMessageStatus) =
            ReprocessResult(id, ReprocessStatus.INVALID_STATUS, "재처리 불가 상태: $status")
        fun maxRetryExceeded(id: Long, retryCount: Int, maxRetry: Int) =
            ReprocessResult(id, ReprocessStatus.MAX_RETRY_EXCEEDED, "최대 재시도 횟수 초과: $retryCount/$maxRetry")
    }
}

data class BatchReprocessResult(
    val total: Int,
    val success: Int,
    val failed: Int,
    val skipped: Int,
    val details: List<ReprocessResult>
) {
    companion object {
        fun empty() = BatchReprocessResult(0, 0, 0, 0, emptyList())
    }
}

data class DltStatistics(
    val pending: Long,
    val retried: Long,
    val resolved: Long,
    val ignored: Long,
    val byTopic: Map<String, Map<FailedMessageStatus, Long>>
) {
    val total: Long get() = pending + retried + resolved + ignored
}