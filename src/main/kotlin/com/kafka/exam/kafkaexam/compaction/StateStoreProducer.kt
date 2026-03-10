package com.kafka.exam.kafkaexam.compaction

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * 상태 저장용 Producer
 *
 * Compacted 토픽에 상태를 저장하고 관리
 * - 키: 엔티티 ID (필수)
 * - 값: 상태 JSON (null이면 삭제 = tombstone)
 */
@Service
class StateStoreProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 상태 저장 (Upsert)
     * 동일 키의 이전 상태는 compaction으로 제거됨
     */
    fun <T> saveState(topic: String, key: String, state: T): CompletableFuture<SendResult<String, String>> {
        val value = objectMapper.writeValueAsString(state)

        log.info("[STATE] Saving state: topic={}, key={}", topic, key)
        log.debug("[STATE] State value: {}", value)

        return kafkaTemplate.send(topic, key, value)
            .whenComplete { result, ex ->
                if (ex != null) {
                    log.error("[STATE] Failed to save state: topic={}, key={}, error={}", topic, key, ex.message)
                } else {
                    log.info(
                        "[STATE] State saved: topic={}, key={}, partition={}, offset={}",
                        topic, key, result.recordMetadata.partition(), result.recordMetadata.offset()
                    )
                }
            }
    }

    /**
     * 상태 삭제 (Tombstone 전송)
     * null 값을 보내면 compaction 시 해당 키 삭제
     */
    fun deleteState(topic: String, key: String): CompletableFuture<SendResult<String, String>> {
        log.info("[STATE] Deleting state (tombstone): topic={}, key={}", topic, key)

        return kafkaTemplate.send(topic, key, null)
            .whenComplete { result, ex ->
                if (ex != null) {
                    log.error("[STATE] Failed to delete state: topic={}, key={}, error={}", topic, key, ex.message)
                } else {
                    log.info(
                        "[STATE] Tombstone sent: topic={}, key={}, partition={}, offset={}",
                        topic, key, result.recordMetadata.partition(), result.recordMetadata.offset()
                    )
                }
            }
    }

    /**
     * 여러 상태 일괄 저장
     */
    fun <T> saveStates(topic: String, states: Map<String, T>): List<CompletableFuture<SendResult<String, String>>> {
        log.info("[STATE] Saving {} states to topic={}", states.size, topic)

        return states.map { (key, state) ->
            saveState(topic, key, state)
        }
    }

    /**
     * 여러 상태 일괄 삭제
     */
    fun deleteStates(topic: String, keys: List<String>): List<CompletableFuture<SendResult<String, String>>> {
        log.info("[STATE] Deleting {} states from topic={}", keys.size, topic)

        return keys.map { key ->
            deleteState(topic, key)
        }
    }

    // === 도메인별 헬퍼 메서드 ===

    fun saveUserState(userId: String, state: UserState): CompletableFuture<SendResult<String, String>> {
        return saveState(CompactedTopicConfig.USER_STATE_TOPIC, userId, state)
    }

    fun deleteUserState(userId: String): CompletableFuture<SendResult<String, String>> {
        return deleteState(CompactedTopicConfig.USER_STATE_TOPIC, userId)
    }

    fun saveProductState(productId: String, state: ProductState): CompletableFuture<SendResult<String, String>> {
        return saveState(CompactedTopicConfig.PRODUCT_STATE_TOPIC, productId, state)
    }

    fun deleteProductState(productId: String): CompletableFuture<SendResult<String, String>> {
        return deleteState(CompactedTopicConfig.PRODUCT_STATE_TOPIC, productId)
    }

    fun saveInventoryState(productId: String, state: InventoryState): CompletableFuture<SendResult<String, String>> {
        return saveState(CompactedTopicConfig.INVENTORY_STATE_TOPIC, productId, state)
    }

    fun saveConfigState(configKey: String, state: ConfigState): CompletableFuture<SendResult<String, String>> {
        return saveState(CompactedTopicConfig.CONFIG_STATE_TOPIC, configKey, state)
    }

    fun deleteConfigState(configKey: String): CompletableFuture<SendResult<String, String>> {
        return deleteState(CompactedTopicConfig.CONFIG_STATE_TOPIC, configKey)
    }
}