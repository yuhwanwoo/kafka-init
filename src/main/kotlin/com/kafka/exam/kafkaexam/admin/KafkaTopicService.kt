package com.kafka.exam.kafkaexam.admin

import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

@Service
class KafkaTopicService(
    private val kafkaAdmin: KafkaAdmin
) {
    private val log = LoggerFactory.getLogger(KafkaTopicService::class.java)
    private val timeoutSeconds = 10L

    private fun getAdminClient(): AdminClient {
        return AdminClient.create(kafkaAdmin.configurationProperties)
    }

    /**
     * 토픽 생성
     */
    fun createTopic(topicName: String, partitions: Int = 3, replicationFactor: Short = 1): Boolean {
        return getAdminClient().use { client ->
            try {
                val newTopic = NewTopic(topicName, partitions, replicationFactor)
                client.createTopics(listOf(newTopic))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)
                log.info("토픽 생성 완료 - name: {}, partitions: {}, replication: {}",
                    topicName, partitions, replicationFactor)
                true
            } catch (e: Exception) {
                log.error("토픽 생성 실패 - name: {}, error: {}", topicName, e.message)
                false
            }
        }
    }

    /**
     * 토픽 삭제
     */
    fun deleteTopic(topicName: String): Boolean {
        return getAdminClient().use { client ->
            try {
                client.deleteTopics(listOf(topicName))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)
                log.info("토픽 삭제 완료 - name: {}", topicName)
                true
            } catch (e: Exception) {
                log.error("토픽 삭제 실패 - name: {}, error: {}", topicName, e.message)
                false
            }
        }
    }

    /**
     * 토픽 존재 여부 확인
     */
    fun topicExists(topicName: String): Boolean {
        return getAdminClient().use { client ->
            try {
                val topics = client.listTopics()
                    .names()
                    .get(timeoutSeconds, TimeUnit.SECONDS)
                topics.contains(topicName)
            } catch (e: Exception) {
                log.error("토픽 존재 확인 실패 - name: {}, error: {}", topicName, e.message)
                false
            }
        }
    }

    /**
     * 전체 토픽 목록 조회
     */
    fun listTopics(): List<String> {
        return getAdminClient().use { client ->
            try {
                client.listTopics()
                    .names()
                    .get(timeoutSeconds, TimeUnit.SECONDS)
                    .toList()
                    .sorted()
            } catch (e: Exception) {
                log.error("토픽 목록 조회 실패 - error: {}", e.message)
                emptyList()
            }
        }
    }

    /**
     * 토픽 상세 정보 조회
     */
    fun describeTopic(topicName: String): TopicInfo? {
        return getAdminClient().use { client ->
            try {
                val description = client.describeTopics(listOf(topicName))
                    .allTopicNames()
                    .get(timeoutSeconds, TimeUnit.SECONDS)[topicName]

                description?.let {
                    TopicInfo(
                        name = it.name(),
                        partitions = it.partitions().size,
                        replicationFactor = it.partitions().firstOrNull()?.replicas()?.size ?: 0,
                        partitionInfos = it.partitions().map { p ->
                            PartitionInfo(
                                partition = p.partition(),
                                leader = p.leader()?.id() ?: -1,
                                replicas = p.replicas().map { r -> r.id() },
                                isr = p.isr().map { r -> r.id() }
                            )
                        }
                    )
                }
            } catch (e: Exception) {
                log.error("토픽 상세 조회 실패 - name: {}, error: {}", topicName, e.message)
                null
            }
        }
    }

    /**
     * 토픽 설정 조회
     */
    fun getTopicConfig(topicName: String): Map<String, String> {
        return getAdminClient().use { client ->
            try {
                val resource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
                val configs = client.describeConfigs(listOf(resource))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                configs[resource]?.entries()
                    ?.filter { !it.isDefault }
                    ?.associate { it.name() to it.value() }
                    ?: emptyMap()
            } catch (e: Exception) {
                log.error("토픽 설정 조회 실패 - name: {}, error: {}", topicName, e.message)
                emptyMap()
            }
        }
    }

    /**
     * 토픽 설정 변경
     */
    fun updateTopicConfig(topicName: String, configs: Map<String, String>): Boolean {
        return getAdminClient().use { client ->
            try {
                val resource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
                val configEntries = configs.map { (key, value) ->
                    AlterConfigOp(ConfigEntry(key, value), AlterConfigOp.OpType.SET)
                }

                client.incrementalAlterConfigs(mapOf(resource to configEntries))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                log.info("토픽 설정 변경 완료 - name: {}, configs: {}", topicName, configs)
                true
            } catch (e: Exception) {
                log.error("토픽 설정 변경 실패 - name: {}, error: {}", topicName, e.message)
                false
            }
        }
    }

    /**
     * 파티션 수 증가 (감소는 불가)
     */
    fun increasePartitions(topicName: String, newPartitionCount: Int): Boolean {
        return getAdminClient().use { client ->
            try {
                val newPartitions = NewPartitions.increaseTo(newPartitionCount)
                client.createPartitions(mapOf(topicName to newPartitions))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                log.info("파티션 증가 완료 - name: {}, newCount: {}", topicName, newPartitionCount)
                true
            } catch (e: Exception) {
                log.error("파티션 증가 실패 - name: {}, error: {}", topicName, e.message)
                false
            }
        }
    }

    /**
     * Consumer Group 목록 조회
     */
    fun listConsumerGroups(): List<String> {
        return getAdminClient().use { client ->
            try {
                client.listConsumerGroups()
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)
                    .map { it.groupId() }
                    .sorted()
            } catch (e: Exception) {
                log.error("Consumer Group 목록 조회 실패 - error: {}", e.message)
                emptyList()
            }
        }
    }

    /**
     * Consumer Group 상세 정보 조회
     */
    fun describeConsumerGroup(groupId: String): ConsumerGroupInfo? {
        return getAdminClient().use { client ->
            try {
                val description = client.describeConsumerGroups(listOf(groupId))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)[groupId]

                description?.let {
                    ConsumerGroupInfo(
                        groupId = it.groupId(),
                        state = it.state().toString(),
                        members = it.members().map { m ->
                            ConsumerMemberInfo(
                                memberId = m.consumerId(),
                                clientId = m.clientId(),
                                host = m.host(),
                                partitions = m.assignment().topicPartitions().map { tp ->
                                    "${tp.topic()}-${tp.partition()}"
                                }
                            )
                        }
                    )
                }
            } catch (e: Exception) {
                log.error("Consumer Group 상세 조회 실패 - groupId: {}, error: {}", groupId, e.message)
                null
            }
        }
    }
}

data class TopicInfo(
    val name: String,
    val partitions: Int,
    val replicationFactor: Int,
    val partitionInfos: List<PartitionInfo>
)

data class PartitionInfo(
    val partition: Int,
    val leader: Int,
    val replicas: List<Int>,
    val isr: List<Int>
)

data class ConsumerGroupInfo(
    val groupId: String,
    val state: String,
    val members: List<ConsumerMemberInfo>
)

data class ConsumerMemberInfo(
    val memberId: String,
    val clientId: String,
    val host: String,
    val partitions: List<String>
)