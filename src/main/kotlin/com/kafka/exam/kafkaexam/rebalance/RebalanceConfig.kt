package com.kafka.exam.kafkaexam.rebalance

import com.kafka.exam.kafkaexam.interceptor.LoggingConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.ContainerProperties

@Configuration
class RebalanceConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    private val consumerStateManager: ConsumerStateManager
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun inMemoryOffsetRepository(): OffsetRepository {
        return InMemoryOffsetRepository()
    }

    /**
     * Rebalance Listener가 적용된 ConsumerFactory
     */
    @Bean
    fun rebalanceAwareConsumerFactory(): ConsumerFactory<String, String> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$groupId-rebalance",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
            // Cooperative 리밸런싱 전략 (점진적 리밸런싱)
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG to
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG to LoggingConsumerInterceptor::class.java.name
        )
        return DefaultKafkaConsumerFactory(config, StringDeserializer(), StringDeserializer())
    }

    /**
     * Rebalance Listener가 적용된 KafkaListenerContainerFactory
     */
    @Bean
    fun rebalanceAwareKafkaListenerContainerFactory(
        offsetRepository: OffsetRepository
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = rebalanceAwareConsumerFactory()
        factory.setConcurrency(3)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

        // ConsumerAwareRebalanceListener 설정
        factory.containerProperties.setConsumerRebalanceListener(
            object : ConsumerAwareRebalanceListener {

                override fun onPartitionsRevokedBeforeCommit(
                    consumer: Consumer<*, *>,
                    partitions: Collection<TopicPartition>
                ) {
                    if (partitions.isEmpty()) return

                    log.info("[REBALANCE] Partitions revoked (before commit): {}", formatPartitions(partitions))

                    // 현재 오프셋 저장
                    for (partition in partitions) {
                        try {
                            val position = consumer.position(partition)
                            offsetRepository.saveOffset(partition, position)
                            log.debug("Offset saved: {}={}", partition, position)
                        } catch (e: Exception) {
                            log.warn("Failed to save offset for {}: {}", partition, e.message)
                        }
                    }

                    // 상태 정리
                    consumerStateManager.cleanupPartitions(partitions)
                    RebalanceMetrics.instance.recordRevocation(partitions.size)
                }

                override fun onPartitionsRevokedAfterCommit(
                    consumer: Consumer<*, *>,
                    partitions: Collection<TopicPartition>
                ) {
                    log.debug("[REBALANCE] Partitions revoked (after commit): {}", formatPartitions(partitions))
                }

                override fun onPartitionsAssigned(
                    consumer: Consumer<*, *>,
                    partitions: Collection<TopicPartition>
                ) {
                    if (partitions.isEmpty()) return

                    log.info("[REBALANCE] Partitions assigned: {}", formatPartitions(partitions))

                    // 저장된 오프셋으로 seek
                    for (partition in partitions) {
                        try {
                            val savedOffset = offsetRepository.getOffset(partition)
                            if (savedOffset != null) {
                                consumer.seek(partition, savedOffset)
                                log.info("Seeking to saved offset: {}={}", partition, savedOffset)
                            }
                        } catch (e: Exception) {
                            log.warn("Failed to seek for {}: {}", partition, e.message)
                        }
                    }

                    // 상태 초기화
                    consumerStateManager.initializePartitions(partitions)
                    RebalanceMetrics.instance.recordAssignment(partitions.size)
                }

                override fun onPartitionsLost(
                    consumer: Consumer<*, *>,
                    partitions: Collection<TopicPartition>
                ) {
                    if (partitions.isEmpty()) return

                    log.warn("[REBALANCE] Partitions lost: {}", formatPartitions(partitions))

                    // 상태만 정리 (오프셋 저장 불가)
                    consumerStateManager.cleanupPartitions(partitions)
                    RebalanceMetrics.instance.recordLost(partitions.size)
                }

                private fun formatPartitions(partitions: Collection<TopicPartition>): String {
                    return partitions.groupBy { it.topic() }
                        .map { (topic, tps) -> "$topic[${tps.map { it.partition() }.sorted().joinToString(",")}]" }
                        .joinToString(", ")
                }
            }
        )

        return factory
    }
}