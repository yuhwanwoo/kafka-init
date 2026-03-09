package com.kafka.exam.kafkaexam.rebalance

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Instant

/**
 * Consumer Rebalance Listener
 *
 * 파티션 재할당 시 다음 작업 수행:
 * - 파티션 해제 전: 오프셋 저장, 진행 중인 작업 정리
 * - 파티션 할당 후: 저장된 오프셋 복구, 상태 초기화
 */
class LoggingRebalanceListener(
    private val consumer: Consumer<*, *>,
    private val offsetRepository: OffsetRepository,
    private val stateManager: ConsumerStateManager
) : ConsumerRebalanceListener {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 파티션이 해제되기 전 호출
     * - 현재 오프셋 저장
     * - 진행 중인 작업 정리
     * - 버퍼/캐시 플러시
     */
    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) {
            return
        }

        log.info("[REBALANCE] Partitions revoked: {}", formatPartitions(partitions))
        val startTime = System.currentTimeMillis()

        try {
            // 1. 현재 오프셋 저장
            saveCurrentOffsets(partitions)

            // 2. 진행 중인 작업 정리
            stateManager.cleanupPartitions(partitions)

            // 3. 메트릭 기록
            RebalanceMetrics.instance.recordRevocation(partitions.size)

            val elapsed = System.currentTimeMillis() - startTime
            log.info(
                "[REBALANCE] Revocation completed: partitions={}, elapsed={}ms",
                partitions.size,
                elapsed
            )

        } catch (e: Exception) {
            log.error("[REBALANCE] Error during partition revocation: {}", e.message, e)
            RebalanceMetrics.instance.recordError("revocation")
        }
    }

    /**
     * 파티션이 할당된 후 호출
     * - 저장된 오프셋에서 시작 위치 결정
     * - 상태 초기화
     */
    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) {
            return
        }

        log.info("[REBALANCE] Partitions assigned: {}", formatPartitions(partitions))
        val startTime = System.currentTimeMillis()

        try {
            // 1. 저장된 오프셋 복구 및 seek
            restoreOffsets(partitions)

            // 2. 파티션별 상태 초기화
            stateManager.initializePartitions(partitions)

            // 3. 메트릭 기록
            RebalanceMetrics.instance.recordAssignment(partitions.size)

            val elapsed = System.currentTimeMillis() - startTime
            log.info(
                "[REBALANCE] Assignment completed: partitions={}, elapsed={}ms",
                partitions.size,
                elapsed
            )

        } catch (e: Exception) {
            log.error("[REBALANCE] Error during partition assignment: {}", e.message, e)
            RebalanceMetrics.instance.recordError("assignment")
        }
    }

    /**
     * 파티션 손실 시 호출 (cooperative rebalancing)
     * onPartitionsRevoked와 달리 오프셋 커밋 기회가 없음
     */
    fun onPartitionsLost(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) {
            return
        }

        log.warn("[REBALANCE] Partitions lost (no commit opportunity): {}", formatPartitions(partitions))

        try {
            // 상태만 정리 (오프셋 저장 불가)
            stateManager.cleanupPartitions(partitions)
            RebalanceMetrics.instance.recordLost(partitions.size)

        } catch (e: Exception) {
            log.error("[REBALANCE] Error handling partition loss: {}", e.message, e)
        }
    }

    private fun saveCurrentOffsets(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            try {
                val currentPosition = consumer.position(partition)
                offsetRepository.saveOffset(partition, currentPosition)

                log.debug(
                    "[REBALANCE] Offset saved: topic={}, partition={}, offset={}",
                    partition.topic(),
                    partition.partition(),
                    currentPosition
                )
            } catch (e: Exception) {
                log.warn(
                    "[REBALANCE] Failed to save offset: topic={}, partition={}, error={}",
                    partition.topic(),
                    partition.partition(),
                    e.message
                )
            }
        }
    }

    private fun restoreOffsets(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            try {
                val savedOffset = offsetRepository.getOffset(partition)

                if (savedOffset != null) {
                    consumer.seek(partition, savedOffset)
                    log.info(
                        "[REBALANCE] Offset restored: topic={}, partition={}, offset={}",
                        partition.topic(),
                        partition.partition(),
                        savedOffset
                    )
                } else {
                    log.debug(
                        "[REBALANCE] No saved offset, using default: topic={}, partition={}",
                        partition.topic(),
                        partition.partition()
                    )
                }
            } catch (e: Exception) {
                log.warn(
                    "[REBALANCE] Failed to restore offset: topic={}, partition={}, error={}",
                    partition.topic(),
                    partition.partition(),
                    e.message
                )
            }
        }
    }

    private fun formatPartitions(partitions: Collection<TopicPartition>): String {
        return partitions.groupBy { it.topic() }
            .map { (topic, tps) -> "$topic[${tps.map { it.partition() }.sorted().joinToString(",")}]" }
            .joinToString(", ")
    }
}