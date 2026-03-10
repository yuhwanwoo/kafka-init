package com.kafka.exam.kafkaexam.compaction

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

/**
 * 상태 저장소 Consumer
 *
 * Compacted 토픽에서 상태를 읽어 로컬 캐시에 저장
 * 애플리케이션 시작 시 전체 상태를 로드하여 메모리에 유지
 */
@Service
class StateStoreConsumer(
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // 로컬 상태 저장소 (실제 운영에서는 Redis 등 사용 권장)
    private val userStateStore = ConcurrentHashMap<String, UserState>()
    private val productStateStore = ConcurrentHashMap<String, ProductState>()
    private val inventoryStateStore = ConcurrentHashMap<String, InventoryState>()
    private val configStateStore = ConcurrentHashMap<String, ConfigState>()

    /**
     * 사용자 상태 Consumer
     */
    @KafkaListener(
        topics = [CompactedTopicConfig.USER_STATE_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}-user-state",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeUserState(record: ConsumerRecord<String, String?>, ack: Acknowledgment) {
        handleStateRecord(record, userStateStore, UserState::class.java, ack)
    }

    /**
     * 상품 상태 Consumer
     */
    @KafkaListener(
        topics = [CompactedTopicConfig.PRODUCT_STATE_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}-product-state",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeProductState(record: ConsumerRecord<String, String?>, ack: Acknowledgment) {
        handleStateRecord(record, productStateStore, ProductState::class.java, ack)
    }

    /**
     * 재고 상태 Consumer
     */
    @KafkaListener(
        topics = [CompactedTopicConfig.INVENTORY_STATE_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}-inventory-state",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeInventoryState(record: ConsumerRecord<String, String?>, ack: Acknowledgment) {
        handleStateRecord(record, inventoryStateStore, InventoryState::class.java, ack)
    }

    /**
     * 설정 상태 Consumer
     */
    @KafkaListener(
        topics = [CompactedTopicConfig.CONFIG_STATE_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}-config-state",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeConfigState(record: ConsumerRecord<String, String?>, ack: Acknowledgment) {
        handleStateRecord(record, configStateStore, ConfigState::class.java, ack)
    }

    /**
     * 상태 레코드 처리 공통 로직
     */
    private fun <T> handleStateRecord(
        record: ConsumerRecord<String, String?>,
        store: ConcurrentHashMap<String, T>,
        clazz: Class<T>,
        ack: Acknowledgment
    ) {
        val key = record.key()
        val value = record.value()

        try {
            if (value == null) {
                // Tombstone: 상태 삭제
                val removed = store.remove(key)
                if (removed != null) {
                    log.info(
                        "[STATE-CONSUMER] State deleted: topic={}, key={}",
                        record.topic(), key
                    )
                }
            } else {
                // 상태 업데이트
                val state = objectMapper.readValue(value, clazz)
                store[key] = state

                log.debug(
                    "[STATE-CONSUMER] State updated: topic={}, key={}, partition={}, offset={}",
                    record.topic(), key, record.partition(), record.offset()
                )
            }

            ack.acknowledge()

        } catch (e: Exception) {
            log.error(
                "[STATE-CONSUMER] Error processing state: topic={}, key={}, error={}",
                record.topic(), key, e.message, e
            )
            throw e
        }
    }

    // === 상태 조회 메서드 ===

    fun getUserState(userId: String): UserState? = userStateStore[userId]

    fun getAllUserStates(): Map<String, UserState> = userStateStore.toMap()

    fun getUserStateCount(): Int = userStateStore.size

    fun getProductState(productId: String): ProductState? = productStateStore[productId]

    fun getAllProductStates(): Map<String, ProductState> = productStateStore.toMap()

    fun getProductStateCount(): Int = productStateStore.size

    fun getInventoryState(productId: String): InventoryState? = inventoryStateStore[productId]

    fun getAllInventoryStates(): Map<String, InventoryState> = inventoryStateStore.toMap()

    fun getInventoryStateCount(): Int = inventoryStateStore.size

    fun getConfigState(configKey: String): ConfigState? = configStateStore[configKey]

    fun getAllConfigStates(): Map<String, ConfigState> = configStateStore.toMap()

    fun getConfigStateCount(): Int = configStateStore.size

    // === 통계 ===

    fun getStoreStats(): StateStoreStats {
        return StateStoreStats(
            userStateCount = userStateStore.size,
            productStateCount = productStateStore.size,
            inventoryStateCount = inventoryStateStore.size,
            configStateCount = configStateStore.size
        )
    }
}

data class StateStoreStats(
    val userStateCount: Int,
    val productStateCount: Int,
    val inventoryStateCount: Int,
    val configStateCount: Int
)