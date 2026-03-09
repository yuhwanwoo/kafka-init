package com.kafka.exam.kafkaexam.rebalance

import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository
import java.util.concurrent.ConcurrentHashMap

/**
 * 오프셋 저장소 인터페이스
 */
interface OffsetRepository {
    fun saveOffset(partition: TopicPartition, offset: Long)
    fun saveOffsets(offsets: Map<TopicPartition, Long>)
    fun getOffset(partition: TopicPartition): Long?
    fun getOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long>
    fun deleteOffset(partition: TopicPartition)
    fun deleteOffsets(partitions: Collection<TopicPartition>)
    fun getAllOffsets(): Map<TopicPartition, Long>
}

/**
 * 메모리 기반 오프셋 저장소 (개발/테스트용)
 */
@Repository
class InMemoryOffsetRepository : OffsetRepository {

    private val log = LoggerFactory.getLogger(javaClass)
    private val offsets = ConcurrentHashMap<String, Long>()

    override fun saveOffset(partition: TopicPartition, offset: Long) {
        val key = toKey(partition)
        offsets[key] = offset
        log.debug("Offset saved: {}={}", key, offset)
    }

    override fun saveOffsets(offsets: Map<TopicPartition, Long>) {
        offsets.forEach { (partition, offset) ->
            saveOffset(partition, offset)
        }
    }

    override fun getOffset(partition: TopicPartition): Long? {
        val key = toKey(partition)
        return offsets[key]
    }

    override fun getOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long> {
        return partitions.mapNotNull { partition ->
            getOffset(partition)?.let { partition to it }
        }.toMap()
    }

    override fun deleteOffset(partition: TopicPartition) {
        val key = toKey(partition)
        offsets.remove(key)
        log.debug("Offset deleted: {}", key)
    }

    override fun deleteOffsets(partitions: Collection<TopicPartition>) {
        partitions.forEach { deleteOffset(it) }
    }

    override fun getAllOffsets(): Map<TopicPartition, Long> {
        return offsets.map { (key, offset) ->
            fromKey(key) to offset
        }.toMap()
    }

    private fun toKey(partition: TopicPartition): String {
        return "${partition.topic()}:${partition.partition()}"
    }

    private fun fromKey(key: String): TopicPartition {
        val parts = key.split(":")
        return TopicPartition(parts[0], parts[1].toInt())
    }
}

/**
 * DB 기반 오프셋 저장소 (운영용)
 * JPA Entity와 Repository를 사용하여 영구 저장
 */
@Repository
class JpaOffsetRepository(
    private val jpaRepository: JpaConsumerOffsetRepository
) : OffsetRepository {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun saveOffset(partition: TopicPartition, offset: Long) {
        val entity = jpaRepository.findByTopicAndPartitionId(partition.topic(), partition.partition())
            ?: ConsumerOffset(
                topic = partition.topic(),
                partitionId = partition.partition(),
                offsetValue = offset
            )

        entity.offsetValue = offset
        entity.updatedAt = java.time.Instant.now()
        jpaRepository.save(entity)

        log.debug("Offset persisted: topic={}, partition={}, offset={}",
            partition.topic(), partition.partition(), offset)
    }

    override fun saveOffsets(offsets: Map<TopicPartition, Long>) {
        offsets.forEach { (partition, offset) ->
            saveOffset(partition, offset)
        }
    }

    override fun getOffset(partition: TopicPartition): Long? {
        return jpaRepository.findByTopicAndPartitionId(partition.topic(), partition.partition())
            ?.offsetValue
    }

    override fun getOffsets(partitions: Collection<TopicPartition>): Map<TopicPartition, Long> {
        return partitions.mapNotNull { partition ->
            getOffset(partition)?.let { partition to it }
        }.toMap()
    }

    override fun deleteOffset(partition: TopicPartition) {
        jpaRepository.deleteByTopicAndPartitionId(partition.topic(), partition.partition())
    }

    override fun deleteOffsets(partitions: Collection<TopicPartition>) {
        partitions.forEach { deleteOffset(it) }
    }

    override fun getAllOffsets(): Map<TopicPartition, Long> {
        return jpaRepository.findAll().associate { entity ->
            TopicPartition(entity.topic, entity.partitionId) to entity.offsetValue
        }
    }
}