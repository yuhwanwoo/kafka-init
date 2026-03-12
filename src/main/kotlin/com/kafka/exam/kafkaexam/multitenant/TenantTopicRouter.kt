package com.kafka.exam.kafkaexam.multitenant

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * 테넌트별 토픽 라우팅 전략
 */
enum class TenantTopicStrategy {
    /**
     * 테넌트별 별도 토픽 (tenant-a-orders, tenant-b-orders)
     */
    TOPIC_PER_TENANT,

    /**
     * 공유 토픽 + 헤더로 테넌트 구분
     */
    SHARED_TOPIC_WITH_HEADER,

    /**
     * 공유 토픽 + 키 접두어로 테넌트 구분
     */
    SHARED_TOPIC_WITH_KEY_PREFIX
}

/**
 * 테넌트 토픽 라우터
 */
@Component
class TenantTopicRouter(
    @Value("\${multitenant.topic.strategy:SHARED_TOPIC_WITH_HEADER}")
    private val defaultStrategy: TenantTopicStrategy,

    @Value("\${multitenant.topic.prefix:tenant}")
    private val topicPrefix: String,

    @Value("\${multitenant.topic.separator:-}")
    private val separator: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 테넌트별 토픽 이름 결정
     */
    fun getTopicForTenant(
        baseTopic: String,
        tenantId: String,
        strategy: TenantTopicStrategy = defaultStrategy
    ): String {
        return when (strategy) {
            TenantTopicStrategy.TOPIC_PER_TENANT -> {
                // tenant-{tenantId}-{baseTopic}
                "$topicPrefix$separator$tenantId$separator$baseTopic"
            }
            TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER,
            TenantTopicStrategy.SHARED_TOPIC_WITH_KEY_PREFIX -> {
                // 공유 토픽 사용
                baseTopic
            }
        }
    }

    /**
     * 테넌트별 메시지 키 생성
     */
    fun getKeyForTenant(
        baseKey: String,
        tenantId: String,
        strategy: TenantTopicStrategy = defaultStrategy
    ): String {
        return when (strategy) {
            TenantTopicStrategy.SHARED_TOPIC_WITH_KEY_PREFIX -> {
                // {tenantId}:{baseKey}
                "$tenantId:$baseKey"
            }
            else -> baseKey
        }
    }

    /**
     * 키에서 테넌트 ID 추출
     */
    fun extractTenantFromKey(key: String): String? {
        return if (key.contains(":")) {
            key.substringBefore(":")
        } else {
            null
        }
    }

    /**
     * 토픽 이름에서 테넌트 ID 추출
     */
    fun extractTenantFromTopic(topic: String): String? {
        // tenant-{tenantId}-{baseTopic} 형식에서 추출
        if (!topic.startsWith("$topicPrefix$separator")) {
            return null
        }

        val withoutPrefix = topic.removePrefix("$topicPrefix$separator")
        return if (withoutPrefix.contains(separator)) {
            withoutPrefix.substringBefore(separator)
        } else {
            null
        }
    }

    /**
     * 테넌트의 모든 토픽 패턴 반환
     */
    fun getTenantTopicPattern(tenantId: String): String {
        return "$topicPrefix$separator$tenantId$separator*"
    }

    /**
     * Consumer Group ID 생성
     */
    fun getConsumerGroupForTenant(baseGroupId: String, tenantId: String): String {
        return "$baseGroupId$separator$tenantId"
    }
}

/**
 * 테넌트 토픽 설정
 */
data class TenantTopicConfig(
    val tenantId: String,
    val baseTopic: String,
    val strategy: TenantTopicStrategy,
    val partitions: Int = 3,
    val replicationFactor: Short = 1
)