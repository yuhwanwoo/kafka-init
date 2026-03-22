package com.kafka.exam.kafkaexam.consumer.dlt

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/dlt/retention")
class DltRetentionController(
    private val retentionService: DltRetentionService
) {

    /**
     * 보존 정책 조회
     * GET /api/dlt/retention/policy
     */
    @GetMapping("/policy")
    fun getRetentionPolicy(): ResponseEntity<RetentionPolicy> {
        return ResponseEntity.ok(retentionService.getRetentionPolicy())
    }

    /**
     * 정리 예상 결과 조회 (dry-run)
     * GET /api/dlt/retention/preview
     */
    @GetMapping("/preview")
    fun previewCleanup(): ResponseEntity<CleanupPreview> {
        return ResponseEntity.ok(retentionService.previewCleanup())
    }

    /**
     * 전체 정리 실행
     * POST /api/dlt/retention/cleanup
     */
    @PostMapping("/cleanup")
    fun runFullCleanup(): ResponseEntity<FullCleanupResult> {
        return ResponseEntity.ok(retentionService.runFullCleanup())
    }

    /**
     * 해결된 메시지 정리
     * POST /api/dlt/retention/cleanup/resolved
     */
    @PostMapping("/cleanup/resolved")
    fun cleanupResolved(): ResponseEntity<CleanupResult> {
        return ResponseEntity.ok(retentionService.cleanupResolved())
    }

    /**
     * 무시된 메시지 정리
     * POST /api/dlt/retention/cleanup/ignored
     */
    @PostMapping("/cleanup/ignored")
    fun cleanupIgnored(): ResponseEntity<CleanupResult> {
        return ResponseEntity.ok(retentionService.cleanupIgnored())
    }

    /**
     * 재처리 완료 메시지 정리
     * POST /api/dlt/retention/cleanup/retried
     */
    @PostMapping("/cleanup/retried")
    fun cleanupRetried(): ResponseEntity<CleanupResult> {
        return ResponseEntity.ok(retentionService.cleanupRetried())
    }

    /**
     * 오래된 PENDING 메시지 아카이브
     * POST /api/dlt/retention/archive/pending
     */
    @PostMapping("/archive/pending")
    fun archiveOldPending(): ResponseEntity<CleanupResult> {
        return ResponseEntity.ok(retentionService.archiveOldPending())
    }

    /**
     * 특정 상태 전체 삭제 (관리자 전용)
     * DELETE /api/dlt/retention/status/{status}
     */
    @DeleteMapping("/status/{status}")
    fun deleteAllByStatus(@PathVariable status: FailedMessageStatus): ResponseEntity<CleanupResult> {
        return ResponseEntity.ok(retentionService.deleteAllByStatus(status))
    }

    /**
     * ID 목록으로 삭제
     * DELETE /api/dlt/retention/messages
     * Body: { "ids": [1, 2, 3] }
     */
    @DeleteMapping("/messages")
    fun deleteByIds(@RequestBody request: DeleteByIdsRequest): ResponseEntity<DeleteResult> {
        val deletedCount = retentionService.deleteByIds(request.ids)
        return ResponseEntity.ok(DeleteResult(deletedCount, request.ids.size))
    }
}

data class DeleteByIdsRequest(
    val ids: List<Long>
)

data class DeleteResult(
    val deletedCount: Int,
    val requestedCount: Int
)
