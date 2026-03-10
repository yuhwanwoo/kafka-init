package com.kafka.exam.kafkaexam.compaction

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

/**
 * Compacted Topic 설정
 *
 * Log Compaction: 각 키의 최신 값만 유지
 * - 상태 저장에 적합 (사용자 프로필, 설정, 재고 현황 등)
 * - 키가 null인 메시지는 tombstone (삭제 마커)
 */
@Configuration
class CompactedTopicConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        // Compacted 토픽 이름
        const val USER_STATE_TOPIC = "user-state"
        const val PRODUCT_STATE_TOPIC = "product-state"
        const val CONFIG_STATE_TOPIC = "config-state"
        const val INVENTORY_STATE_TOPIC = "inventory-state"
    }

    /**
     * 사용자 상태 저장용 Compacted 토픽
     */
    @Bean
    fun userStateTopic(): NewTopic {
        return createCompactedTopic(
            name = USER_STATE_TOPIC,
            partitions = 6,
            retentionMs = -1,  // 무제한 보관
            minCleanableDirtyRatio = 0.5,
            deleteRetentionMs = 86400000  // tombstone 24시간 보관
        )
    }

    /**
     * 상품 상태 저장용 Compacted 토픽
     */
    @Bean
    fun productStateTopic(): NewTopic {
        return createCompactedTopic(
            name = PRODUCT_STATE_TOPIC,
            partitions = 6,
            retentionMs = -1,
            minCleanableDirtyRatio = 0.3,
            deleteRetentionMs = 86400000
        )
    }

    /**
     * 설정 저장용 Compacted 토픽
     */
    @Bean
    fun configStateTopic(): NewTopic {
        return createCompactedTopic(
            name = CONFIG_STATE_TOPIC,
            partitions = 3,
            retentionMs = -1,
            minCleanableDirtyRatio = 0.1,  // 자주 정리
            deleteRetentionMs = 3600000    // tombstone 1시간 보관
        )
    }

    /**
     * 재고 상태 저장용 Compacted 토픽
     */
    @Bean
    fun inventoryStateTopic(): NewTopic {
        return createCompactedTopic(
            name = INVENTORY_STATE_TOPIC,
            partitions = 6,
            retentionMs = -1,
            minCleanableDirtyRatio = 0.5,
            deleteRetentionMs = 86400000
        )
    }

    /**
     * Compacted 토픽 생성 헬퍼
     */
    private fun createCompactedTopic(
        name: String,
        partitions: Int,
        retentionMs: Long,
        minCleanableDirtyRatio: Double,
        deleteRetentionMs: Long,
        segmentMs: Long = 3600000,  // 1시간마다 세그먼트 롤링
        segmentBytes: Int = 104857600  // 100MB
    ): NewTopic {
        log.info(
            "Creating compacted topic: name={}, partitions={}, minCleanableDirtyRatio={}",
            name, partitions, minCleanableDirtyRatio
        )

        return TopicBuilder.name(name)
            .partitions(partitions)
            .replicas(1)  // 개발 환경, 운영에서는 3 권장
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
            .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString())
            .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, minCleanableDirtyRatio.toString())
            .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, deleteRetentionMs.toString())
            .config(TopicConfig.SEGMENT_MS_CONFIG, segmentMs.toString())
            .config(TopicConfig.SEGMENT_BYTES_CONFIG, segmentBytes.toString())
            // Compaction 최적화 설정
            .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")  // 즉시 compaction 허용
            .config(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "86400000")  // 최대 24시간
            .build()
    }

    /**
     * Compact + Delete 정책 토픽 생성 (시간 기반 삭제 + Compaction)
     */
    fun createCompactDeleteTopic(
        name: String,
        partitions: Int,
        retentionMs: Long = 604800000  // 7일
    ): NewTopic {
        return TopicBuilder.name(name)
            .partitions(partitions)
            .replicas(1)
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")
            .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString())
            .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5")
            .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "86400000")
            .build()
    }
}