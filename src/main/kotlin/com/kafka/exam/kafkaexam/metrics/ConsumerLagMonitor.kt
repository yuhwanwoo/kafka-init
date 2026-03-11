package com.kafka.exam.kafkaexam.metrics

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy

/**
 * Consumer Lag 모니터
 *
 * Consumer Group별 Lag 추적 및 알림
 */
@Component
class ConsumerLagMonitor(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val defaultGroupId: String,
    @Value("\${kafka.metrics.lag.warning-threshold:1000}") private val warningThreshold: Long,
    @Value("\${kafka.metrics.lag.critical-threshold:10000}") private val criticalThreshold: Long
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private lateinit var adminClient: AdminClient

    // Lag 히스토리 (트렌드 분석용)
    private val lagHistory = ConcurrentHashMap<String, MutableList<LagSnapshot>>()
    private val maxHistorySize = 60  // 최근 60개 스냅샷 유지

    // 알림 상태 (중복 알림 방지)
    private val alertState = ConcurrentHashMap<String, AlertLevel>()

    @PostConstruct
    fun init() {
        adminClient = AdminClient.create(mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
        ))
        log.info("ConsumerLagMonitor initialized: warningThreshold={}, criticalThreshold={}",
            warningThreshold, criticalThreshold)
    }

    @PreDestroy
    fun cleanup() {
        adminClient.close()
    }

    /**
     * 전체 Consumer Group의 Lag 수집
     */
    @Scheduled(fixedDelayString = "\${kafka.metrics.lag.interval:30000}")
    fun collectAllLags() {
        try {
            val consumerGroups = adminClient.listConsumerGroups().all().get()

            for (group in consumerGroups) {
                collectGroupLag(group.groupId())
            }
        } catch (e: Exception) {
            log.error("Failed to collect consumer lags: {}", e.message)
        }
    }

    /**
     * 특정 Consumer Group의 Lag 수집
     */
    fun collectGroupLag(groupId: String): GroupLagInfo? {
        return try {
            val offsets = adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get()

            if (offsets.isEmpty()) return null

            val partitionLags = mutableMapOf<TopicPartition, PartitionLagInfo>()
            val topicPartitions = offsets.keys.toList()
            val endOffsets = getEndOffsets(topicPartitions)

            var totalLag = 0L

            for ((tp, offsetAndMetadata) in offsets) {
                val endOffset = endOffsets[tp] ?: continue
                val currentOffset = offsetAndMetadata.offset()
                val lag = (endOffset - currentOffset).coerceAtLeast(0)

                partitionLags[tp] = PartitionLagInfo(
                    topic = tp.topic(),
                    partition = tp.partition(),
                    currentOffset = currentOffset,
                    endOffset = endOffset,
                    lag = lag
                )

                totalLag += lag

                // 알림 체크
                checkAndAlert(groupId, tp, lag)
            }

            val groupLagInfo = GroupLagInfo(
                groupId = groupId,
                totalLag = totalLag,
                partitionLags = partitionLags.values.toList(),
                timestamp = Instant.now()
            )

            // 히스토리에 추가
            addToHistory(groupId, LagSnapshot(totalLag, Instant.now()))

            groupLagInfo

        } catch (e: Exception) {
            log.error("Failed to collect lag for group {}: {}", groupId, e.message)
            null
        }
    }

    private fun getEndOffsets(partitions: List<TopicPartition>): Map<TopicPartition, Long> {
        if (partitions.isEmpty()) return emptyMap()

        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$defaultGroupId-lag-monitor-temp",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )

        KafkaConsumer<String, String>(props).use { consumer ->
            return consumer.endOffsets(partitions)
        }
    }

    private fun checkAndAlert(groupId: String, tp: TopicPartition, lag: Long) {
        val key = "$groupId-${tp.topic()}-${tp.partition()}"
        val previousLevel = alertState[key] ?: AlertLevel.NORMAL

        val currentLevel = when {
            lag >= criticalThreshold -> AlertLevel.CRITICAL
            lag >= warningThreshold -> AlertLevel.WARNING
            else -> AlertLevel.NORMAL
        }

        // 레벨이 변경되었을 때만 로그
        if (currentLevel != previousLevel) {
            when (currentLevel) {
                AlertLevel.CRITICAL -> {
                    log.error(
                        "[ALERT-CRITICAL] High consumer lag: group={}, topic={}, partition={}, lag={}",
                        groupId, tp.topic(), tp.partition(), lag
                    )
                }
                AlertLevel.WARNING -> {
                    log.warn(
                        "[ALERT-WARNING] Consumer lag warning: group={}, topic={}, partition={}, lag={}",
                        groupId, tp.topic(), tp.partition(), lag
                    )
                }
                AlertLevel.NORMAL -> {
                    if (previousLevel != AlertLevel.NORMAL) {
                        log.info(
                            "[ALERT-RESOLVED] Consumer lag normalized: group={}, topic={}, partition={}, lag={}",
                            groupId, tp.topic(), tp.partition(), lag
                        )
                    }
                }
            }
            alertState[key] = currentLevel
        }
    }

    private fun addToHistory(groupId: String, snapshot: LagSnapshot) {
        val history = lagHistory.computeIfAbsent(groupId) { mutableListOf() }
        history.add(snapshot)

        // 오래된 스냅샷 제거
        while (history.size > maxHistorySize) {
            history.removeAt(0)
        }
    }

    /**
     * Lag 트렌드 분석
     */
    fun analyzeTrend(groupId: String): LagTrend? {
        val history = lagHistory[groupId] ?: return null
        if (history.size < 2) return null

        val recentLags = history.takeLast(10).map { it.lag }
        val avgLag = recentLags.average()
        val currentLag = recentLags.last()

        val trend = when {
            currentLag > avgLag * 1.5 -> TrendDirection.INCREASING
            currentLag < avgLag * 0.5 -> TrendDirection.DECREASING
            else -> TrendDirection.STABLE
        }

        return LagTrend(
            groupId = groupId,
            currentLag = currentLag,
            averageLag = avgLag,
            minLag = recentLags.minOrNull() ?: 0,
            maxLag = recentLags.maxOrNull() ?: 0,
            trend = trend,
            sampleCount = recentLags.size
        )
    }

    /**
     * 모든 그룹의 Lag 정보 조회
     */
    fun getAllGroupLags(): List<GroupLagInfo> {
        return try {
            val consumerGroups = adminClient.listConsumerGroups().all().get()
            consumerGroups.mapNotNull { collectGroupLag(it.groupId()) }
        } catch (e: Exception) {
            log.error("Failed to get all group lags: {}", e.message)
            emptyList()
        }
    }

    /**
     * 알림 상태 조회
     */
    fun getAlertStates(): Map<String, AlertLevel> = alertState.toMap()

    /**
     * 특정 레벨 이상의 알림 조회
     */
    fun getActiveAlerts(minLevel: AlertLevel = AlertLevel.WARNING): Map<String, AlertLevel> {
        return alertState.filter { it.value >= minLevel }
    }
}

data class GroupLagInfo(
    val groupId: String,
    val totalLag: Long,
    val partitionLags: List<PartitionLagInfo>,
    val timestamp: Instant
)

data class PartitionLagInfo(
    val topic: String,
    val partition: Int,
    val currentOffset: Long,
    val endOffset: Long,
    val lag: Long
)

data class LagSnapshot(
    val lag: Long,
    val timestamp: Instant
)

data class LagTrend(
    val groupId: String,
    val currentLag: Long,
    val averageLag: Double,
    val minLag: Long,
    val maxLag: Long,
    val trend: TrendDirection,
    val sampleCount: Int
)

enum class TrendDirection {
    INCREASING, DECREASING, STABLE
}

enum class AlertLevel {
    NORMAL, WARNING, CRITICAL
}