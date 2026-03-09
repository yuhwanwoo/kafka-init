package com.kafka.exam.kafkaexam.rebalance

import org.apache.kafka.common.TopicPartition
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/rebalance")
class RebalanceController(
    private val consumerStateManager: ConsumerStateManager,
    private val offsetRepository: OffsetRepository
) {
    /**
     * Rebalance 메트릭 조회
     */
    @GetMapping("/metrics")
    fun getMetrics(): ResponseEntity<RebalanceStats> {
        return ResponseEntity.ok(RebalanceMetrics.instance.getStats())
    }

    /**
     * Rebalance 히스토리 조회
     */
    @GetMapping("/history")
    fun getHistory(
        @RequestParam(defaultValue = "20") count: Int
    ): ResponseEntity<List<RebalanceEvent>> {
        return ResponseEntity.ok(RebalanceMetrics.instance.getRecentHistory(count))
    }

    /**
     * 메트릭 리셋
     */
    @DeleteMapping("/metrics")
    fun resetMetrics(): ResponseEntity<Map<String, String>> {
        RebalanceMetrics.instance.reset()
        return ResponseEntity.ok(mapOf("message" to "Metrics reset successfully"))
    }

    /**
     * 모든 파티션 상태 조회
     */
    @GetMapping("/partitions")
    fun getAllPartitionStates(): ResponseEntity<List<PartitionStateDto>> {
        val states = consumerStateManager.getAllPartitionStates()
        return ResponseEntity.ok(states.map { (partition, state) ->
            PartitionStateDto(
                topic = partition.topic(),
                partition = partition.partition(),
                status = state.status.name,
                lastOffset = state.lastOffset,
                processedCount = state.processedCount.get(),
                errorCount = state.errorCount.get(),
                lastError = state.lastError,
                assignedAt = state.assignedAt,
                bufferedMessages = consumerStateManager.getBufferedMessageCount(partition)
            )
        })
    }

    /**
     * 특정 파티션 상태 조회
     */
    @GetMapping("/partitions/{topic}/{partition}")
    fun getPartitionState(
        @PathVariable topic: String,
        @PathVariable partition: Int
    ): ResponseEntity<PartitionStateDto> {
        val tp = TopicPartition(topic, partition)
        val state = consumerStateManager.getPartitionState(tp)
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(
            PartitionStateDto(
                topic = topic,
                partition = partition,
                status = state.status.name,
                lastOffset = state.lastOffset,
                processedCount = state.processedCount.get(),
                errorCount = state.errorCount.get(),
                lastError = state.lastError,
                assignedAt = state.assignedAt,
                bufferedMessages = consumerStateManager.getBufferedMessageCount(tp)
            )
        )
    }

    /**
     * 저장된 오프셋 조회
     */
    @GetMapping("/offsets")
    fun getAllOffsets(): ResponseEntity<List<OffsetDto>> {
        val offsets = offsetRepository.getAllOffsets()
        return ResponseEntity.ok(offsets.map { (partition, offset) ->
            OffsetDto(
                topic = partition.topic(),
                partition = partition.partition(),
                offset = offset
            )
        })
    }

    /**
     * 특정 토픽/파티션 오프셋 조회
     */
    @GetMapping("/offsets/{topic}/{partition}")
    fun getOffset(
        @PathVariable topic: String,
        @PathVariable partition: Int
    ): ResponseEntity<OffsetDto> {
        val tp = TopicPartition(topic, partition)
        val offset = offsetRepository.getOffset(tp)
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(
            OffsetDto(
                topic = topic,
                partition = partition,
                offset = offset
            )
        )
    }

    /**
     * 비활성 파티션 조회
     */
    @GetMapping("/partitions/inactive")
    fun getInactivePartitions(
        @RequestParam(defaultValue = "60000") thresholdMs: Long
    ): ResponseEntity<List<String>> {
        val inactive = consumerStateManager.findInactivePartitions(thresholdMs)
        return ResponseEntity.ok(inactive.map { "${it.topic()}-${it.partition()}" })
    }

    /**
     * 버퍼 상태 요약
     */
    @GetMapping("/buffer/summary")
    fun getBufferSummary(): ResponseEntity<BufferSummary> {
        val states = consumerStateManager.getAllPartitionStates()
        val totalBuffered = consumerStateManager.getTotalBufferedMessageCount()

        return ResponseEntity.ok(
            BufferSummary(
                totalPartitions = states.size,
                activePartitions = states.count { it.value.status == PartitionStatus.ACTIVE },
                processingPartitions = states.count { it.value.status == PartitionStatus.PROCESSING },
                errorPartitions = states.count { it.value.status == PartitionStatus.ERROR },
                totalBufferedMessages = totalBuffered
            )
        )
    }
}

data class PartitionStateDto(
    val topic: String,
    val partition: Int,
    val status: String,
    val lastOffset: Long,
    val processedCount: Long,
    val errorCount: Long,
    val lastError: String?,
    val assignedAt: Long,
    val bufferedMessages: Int
)

data class OffsetDto(
    val topic: String,
    val partition: Int,
    val offset: Long
)

data class BufferSummary(
    val totalPartitions: Int,
    val activePartitions: Int,
    val processingPartitions: Int,
    val errorPartitions: Int,
    val totalBufferedMessages: Int
)