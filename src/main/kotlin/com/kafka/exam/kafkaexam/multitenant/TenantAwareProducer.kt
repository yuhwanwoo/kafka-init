package com.kafka.exam.kafkaexam.multitenant

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture

/**
 * 테넌트 인식 Producer
 *
 * 테넌트 정보를 헤더에 추가하고, 전략에 따라 토픽/키 라우팅
 */
@Service
class TenantAwareProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val topicRouter: TenantTopicRouter,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 현재 테넌트 컨텍스트로 메시지 전송
     */
    fun send(
        baseTopic: String,
        key: String,
        message: Any,
        strategy: TenantTopicStrategy = TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER
    ): CompletableFuture<SendResult<String, String>> {
        val tenantId = TenantContext.requireTenantId()
        return sendForTenant(tenantId, baseTopic, key, message, strategy)
    }

    /**
     * 특정 테넌트로 메시지 전송
     */
    fun sendForTenant(
        tenantId: String,
        baseTopic: String,
        key: String,
        message: Any,
        strategy: TenantTopicStrategy = TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER
    ): CompletableFuture<SendResult<String, String>> {

        val topic = topicRouter.getTopicForTenant(baseTopic, tenantId, strategy)
        val routedKey = topicRouter.getKeyForTenant(key, tenantId, strategy)
        val value = if (message is String) message else objectMapper.writeValueAsString(message)

        val record = ProducerRecord<String, String>(topic, routedKey, value)

        // 테넌트 헤더 추가
        record.headers().add(
            RecordHeader(TenantContext.TENANT_ID_HEADER, tenantId.toByteArray(StandardCharsets.UTF_8))
        )

        log.info(
            "[TENANT-PRODUCER] Sending: tenant={}, topic={}, key={}, strategy={}",
            tenantId, topic, routedKey, strategy
        )

        return kafkaTemplate.send(record)
            .whenComplete { result, ex ->
                if (ex != null) {
                    log.error(
                        "[TENANT-PRODUCER] Failed: tenant={}, topic={}, error={}",
                        tenantId, topic, ex.message
                    )
                } else {
                    log.debug(
                        "[TENANT-PRODUCER] Sent: tenant={}, topic={}, partition={}, offset={}",
                        tenantId, topic, result.recordMetadata.partition(), result.recordMetadata.offset()
                    )
                }
            }
    }

    /**
     * 여러 테넌트에 브로드캐스트
     */
    fun broadcast(
        tenantIds: List<String>,
        baseTopic: String,
        key: String,
        message: Any,
        strategy: TenantTopicStrategy = TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER
    ): List<CompletableFuture<SendResult<String, String>>> {
        log.info("[TENANT-PRODUCER] Broadcasting to {} tenants", tenantIds.size)

        return tenantIds.map { tenantId ->
            sendForTenant(tenantId, baseTopic, key, message, strategy)
        }
    }

    /**
     * 배치 전송
     */
    fun sendBatch(
        tenantId: String,
        baseTopic: String,
        messages: List<Pair<String, Any>>,
        strategy: TenantTopicStrategy = TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER
    ): List<CompletableFuture<SendResult<String, String>>> {
        log.info("[TENANT-PRODUCER] Batch sending {} messages for tenant={}", messages.size, tenantId)

        return messages.map { (key, message) ->
            sendForTenant(tenantId, baseTopic, key, message, strategy)
        }
    }
}