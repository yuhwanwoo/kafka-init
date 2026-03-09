package com.kafka.exam.kafkaexam.rebalance

import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Consumer 상태 관리자
 *
 * 파티션별 처리 상태, 버퍼, 캐시 등을 관리
 */
@Component
class ConsumerStateManager {

    private val log = LoggerFactory.getLogger(javaClass)

    // 파티션별 처리 상태
    private val partitionStates = ConcurrentHashMap<TopicPartition, PartitionState>()

    // 파티션별 처리 중인 메시지 버퍼
    private val messageBuffers = ConcurrentHashMap<TopicPartition, MutableList<BufferedMessage>>()

    // 파티션별 마지막 처리 시간
    private val lastProcessedTime = ConcurrentHashMap<TopicPartition, Long>()

    /**
     * 파티션 초기화
     */
    fun initializePartitions(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            partitionStates[partition] = PartitionState(
                partition = partition,
                status = PartitionStatus.ACTIVE,
                assignedAt = System.currentTimeMillis()
            )
            messageBuffers[partition] = mutableListOf()
            lastProcessedTime[partition] = System.currentTimeMillis()

            log.debug("Partition initialized: {}", partition)
        }
    }

    /**
     * 파티션 정리
     */
    fun cleanupPartitions(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            // 버퍼에 남은 메시지 처리 또는 로깅
            val bufferedMessages = messageBuffers.remove(partition)
            if (!bufferedMessages.isNullOrEmpty()) {
                log.warn(
                    "Partition revoked with {} buffered messages: {}",
                    bufferedMessages.size,
                    partition
                )
                // 필요시 DLT로 전송하거나 별도 저장
            }

            // 상태 정리
            val state = partitionStates.remove(partition)
            state?.let {
                val duration = System.currentTimeMillis() - it.assignedAt
                log.info(
                    "Partition cleaned up: {}, duration={}ms, processed={}",
                    partition,
                    duration,
                    it.processedCount.get()
                )
            }

            lastProcessedTime.remove(partition)
        }
    }

    /**
     * 메시지 처리 시작
     */
    fun onMessageReceived(partition: TopicPartition, offset: Long, key: String?) {
        val buffer = messageBuffers[partition] ?: return
        buffer.add(BufferedMessage(offset, key, System.currentTimeMillis()))

        partitionStates[partition]?.let {
            it.lastOffset = offset
            it.status = PartitionStatus.PROCESSING
        }
    }

    /**
     * 메시지 처리 완료
     */
    fun onMessageProcessed(partition: TopicPartition, offset: Long) {
        val buffer = messageBuffers[partition] ?: return
        buffer.removeIf { it.offset == offset }

        partitionStates[partition]?.let {
            it.processedCount.incrementAndGet()
            it.status = PartitionStatus.ACTIVE
        }

        lastProcessedTime[partition] = System.currentTimeMillis()
    }

    /**
     * 메시지 처리 실패
     */
    fun onMessageFailed(partition: TopicPartition, offset: Long, error: String) {
        partitionStates[partition]?.let {
            it.errorCount.incrementAndGet()
            it.lastError = error
            it.status = PartitionStatus.ERROR
        }

        log.warn("Message processing failed: partition={}, offset={}, error={}", partition, offset, error)
    }

    /**
     * 파티션 상태 조회
     */
    fun getPartitionState(partition: TopicPartition): PartitionState? {
        return partitionStates[partition]
    }

    /**
     * 모든 파티션 상태 조회
     */
    fun getAllPartitionStates(): Map<TopicPartition, PartitionState> {
        return partitionStates.toMap()
    }

    /**
     * 버퍼에 남은 메시지 수 조회
     */
    fun getBufferedMessageCount(partition: TopicPartition): Int {
        return messageBuffers[partition]?.size ?: 0
    }

    /**
     * 전체 버퍼 메시지 수 조회
     */
    fun getTotalBufferedMessageCount(): Int {
        return messageBuffers.values.sumOf { it.size }
    }

    /**
     * 파티션 일시정지
     */
    fun pausePartition(partition: TopicPartition) {
        partitionStates[partition]?.status = PartitionStatus.PAUSED
        log.info("Partition paused: {}", partition)
    }

    /**
     * 파티션 재개
     */
    fun resumePartition(partition: TopicPartition) {
        partitionStates[partition]?.status = PartitionStatus.ACTIVE
        log.info("Partition resumed: {}", partition)
    }

    /**
     * 비활성 파티션 감지 (처리가 오래 멈춘 파티션)
     */
    fun findInactivePartitions(thresholdMs: Long): List<TopicPartition> {
        val now = System.currentTimeMillis()
        return lastProcessedTime.filter { (_, lastTime) ->
            now - lastTime > thresholdMs
        }.keys.toList()
    }
}

data class PartitionState(
    val partition: TopicPartition,
    var status: PartitionStatus,
    val assignedAt: Long,
    var lastOffset: Long = -1,
    val processedCount: AtomicLong = AtomicLong(0),
    val errorCount: AtomicLong = AtomicLong(0),
    var lastError: String? = null
)

enum class PartitionStatus {
    ACTIVE,      // 정상 처리 중
    PROCESSING,  // 메시지 처리 중
    PAUSED,      // 일시정지
    ERROR,       // 에러 발생
    REVOKED      // 해제됨
}

data class BufferedMessage(
    val offset: Long,
    val key: String?,
    val receivedAt: Long
)