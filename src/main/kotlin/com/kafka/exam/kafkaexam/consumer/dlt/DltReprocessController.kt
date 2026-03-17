package com.kafka.exam.kafkaexam.consumer.dlt

import org.springframework.data.domain.Page
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/dlt")
class DltReprocessController(
    private val dltReprocessService: DltReprocessService
) {

    /**
     * 실패 메시지 목록 조회
     * GET /api/dlt/messages?status=PENDING&topic=xxx&page=0&size=20
     */
    @GetMapping("/messages")
    fun getMessages(
        @RequestParam(required = false) status: FailedMessageStatus?,
        @RequestParam(required = false) topic: String?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int
    ): ResponseEntity<Page<FailedMessageDto>> {
        val messages = dltReprocessService.getFailedMessages(status, topic, page, size)
        return ResponseEntity.ok(messages.map { it.toDto() })
    }

    /**
     * 실패 메시지 상세 조회
     * GET /api/dlt/messages/{id}
     */
    @GetMapping("/messages/{id}")
    fun getMessage(@PathVariable id: Long): ResponseEntity<FailedMessageDto> {
        val message = dltReprocessService.getFailedMessages(page = 0, size = 1)
            .content.find { it.id == id }
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(message.toDto())
    }

    /**
     * 통계 조회
     * GET /api/dlt/statistics
     */
    @GetMapping("/statistics")
    fun getStatistics(): ResponseEntity<DltStatistics> {
        return ResponseEntity.ok(dltReprocessService.getStatistics())
    }

    /**
     * 단건 재처리
     * POST /api/dlt/reprocess/{id}
     */
    @PostMapping("/reprocess/{id}")
    fun reprocess(@PathVariable id: Long): ResponseEntity<ReprocessResult> {
        val result = dltReprocessService.reprocess(id)
        return when (result.status) {
            ReprocessStatus.SUCCESS -> ResponseEntity.ok(result)
            ReprocessStatus.NOT_FOUND -> ResponseEntity.notFound().build()
            else -> ResponseEntity.badRequest().body(result)
        }
    }

    /**
     * 다건 재처리
     * POST /api/dlt/reprocess/batch
     * Body: { "ids": [1, 2, 3] }
     */
    @PostMapping("/reprocess/batch")
    fun reprocessBatch(@RequestBody request: BatchReprocessRequest): ResponseEntity<BatchReprocessResult> {
        val result = dltReprocessService.reprocessBatch(request.ids)
        return ResponseEntity.ok(result)
    }

    /**
     * 토픽별 재처리
     * POST /api/dlt/reprocess/topic/{topic}?limit=100
     */
    @PostMapping("/reprocess/topic/{topic}")
    fun reprocessByTopic(
        @PathVariable topic: String,
        @RequestParam(defaultValue = "100") limit: Int
    ): ResponseEntity<BatchReprocessResult> {
        val result = dltReprocessService.reprocessByTopic(topic, limit)
        return ResponseEntity.ok(result)
    }

    /**
     * 모든 PENDING 메시지 재처리 (수동 트리거)
     * POST /api/dlt/reprocess/all?batchSize=50
     */
    @PostMapping("/reprocess/all")
    fun reprocessAll(
        @RequestParam(defaultValue = "50") batchSize: Int
    ): ResponseEntity<BatchReprocessResult> {
        val result = dltReprocessService.reprocessAllPending(batchSize)
        return ResponseEntity.ok(result)
    }

    /**
     * 무시 처리
     * POST /api/dlt/messages/{id}/ignore
     */
    @PostMapping("/messages/{id}/ignore")
    fun ignore(@PathVariable id: Long): ResponseEntity<StatusChangeResponse> {
        val success = dltReprocessService.ignore(id)
        return if (success) {
            ResponseEntity.ok(StatusChangeResponse(id, FailedMessageStatus.IGNORED, true))
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * 수동 해결 처리
     * POST /api/dlt/messages/{id}/resolve
     */
    @PostMapping("/messages/{id}/resolve")
    fun resolve(@PathVariable id: Long): ResponseEntity<StatusChangeResponse> {
        val success = dltReprocessService.resolve(id)
        return if (success) {
            ResponseEntity.ok(StatusChangeResponse(id, FailedMessageStatus.RESOLVED, true))
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * 벌크 상태 변경
     * POST /api/dlt/messages/bulk-status
     * Body: { "ids": [1, 2, 3], "status": "IGNORED" }
     */
    @PostMapping("/messages/bulk-status")
    fun bulkUpdateStatus(@RequestBody request: BulkStatusUpdateRequest): ResponseEntity<BulkStatusUpdateResponse> {
        val updatedCount = dltReprocessService.bulkUpdateStatus(request.ids, request.status)
        return ResponseEntity.ok(BulkStatusUpdateResponse(updatedCount, request.status))
    }
}

// DTOs
data class FailedMessageDto(
    val id: Long,
    val originalTopic: String,
    val partitionId: Int,
    val offsetId: Long,
    val messageKey: String?,
    val messageValue: String?,
    val errorMessage: String?,
    val status: FailedMessageStatus,
    val createdAt: String,
    val resolvedAt: String?,
    val retryCount: Int
)

fun FailedMessage.toDto() = FailedMessageDto(
    id = id ?: 0,
    originalTopic = originalTopic,
    partitionId = partitionId,
    offsetId = offsetId,
    messageKey = messageKey,
    messageValue = messageValue,
    errorMessage = errorMessage,
    status = status,
    createdAt = createdAt.toString(),
    resolvedAt = resolvedAt?.toString(),
    retryCount = retryCount
)

data class BatchReprocessRequest(
    val ids: List<Long>
)

data class StatusChangeResponse(
    val id: Long,
    val newStatus: FailedMessageStatus,
    val success: Boolean
)

data class BulkStatusUpdateRequest(
    val ids: List<Long>,
    val status: FailedMessageStatus
)

data class BulkStatusUpdateResponse(
    val updatedCount: Int,
    val status: FailedMessageStatus
)