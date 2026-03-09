package com.kafka.exam.kafkaexam.rebalance

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Index
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.Instant

/**
 * Consumer Offset 저장 엔티티
 */
@Entity
@Table(
    name = "consumer_offsets",
    indexes = [
        Index(name = "idx_topic_partition", columnList = "topic, partition_id", unique = true)
    ]
)
class ConsumerOffset(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @Column(nullable = false)
    val topic: String,

    @Column(name = "partition_id", nullable = false)
    val partitionId: Int,

    @Column(name = "offset_value", nullable = false)
    var offsetValue: Long,

    @Column(name = "consumer_group")
    var consumerGroup: String? = null,

    @Column(name = "created_at", nullable = false)
    val createdAt: Instant = Instant.now(),

    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.now()
)

@Repository
interface JpaConsumerOffsetRepository : JpaRepository<ConsumerOffset, Long> {
    fun findByTopicAndPartitionId(topic: String, partitionId: Int): ConsumerOffset?
    fun findByTopic(topic: String): List<ConsumerOffset>
    fun findByConsumerGroup(consumerGroup: String): List<ConsumerOffset>
    fun deleteByTopicAndPartitionId(topic: String, partitionId: Int)
    fun deleteByTopic(topic: String)
}