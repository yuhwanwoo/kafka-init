package com.kafka.exam.kafkaexam.partitioner

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils
import org.slf4j.LoggerFactory

/**
 * 우선순위 기반 Custom Partitioner
 *
 * 메시지 키의 우선순위 접두어에 따라 파티션 할당:
 * - "HIGH:" 또는 "P1:" -> 전용 고우선순위 파티션 (파티션 0)
 * - "MEDIUM:" 또는 "P2:" -> 중간 우선순위 파티션 (파티션 1-2)
 * - "LOW:" 또는 "P3:" -> 낮은 우선순위 파티션 (나머지)
 *
 * 고우선순위 메시지는 전용 파티션에서 빠르게 처리됨
 */
class PriorityPartitioner : Partitioner {

    private val log = LoggerFactory.getLogger(javaClass)

    private var highPriorityPartition = 0
    private var mediumPriorityPartitionStart = 1
    private var mediumPriorityPartitionCount = 2

    companion object {
        const val HIGH_PRIORITY_PARTITION = "partitioner.priority.high.partition"
        const val MEDIUM_PRIORITY_PARTITION_START = "partitioner.priority.medium.partition.start"
        const val MEDIUM_PRIORITY_PARTITION_COUNT = "partitioner.priority.medium.partition.count"

        private val HIGH_PRIORITY_PREFIXES = listOf("HIGH:", "P1:", "URGENT:", "CRITICAL:")
        private val MEDIUM_PRIORITY_PREFIXES = listOf("MEDIUM:", "P2:", "NORMAL:")
        private val LOW_PRIORITY_PREFIXES = listOf("LOW:", "P3:", "BATCH:")
    }

    override fun configure(configs: MutableMap<String, *>) {
        highPriorityPartition = configs[HIGH_PRIORITY_PARTITION]?.toString()?.toIntOrNull() ?: 0
        mediumPriorityPartitionStart = configs[MEDIUM_PRIORITY_PARTITION_START]?.toString()?.toIntOrNull() ?: 1
        mediumPriorityPartitionCount = configs[MEDIUM_PRIORITY_PARTITION_COUNT]?.toString()?.toIntOrNull() ?: 2

        log.info(
            "PriorityPartitioner configured: high={}, medium=[{}-{}]",
            highPriorityPartition,
            mediumPriorityPartitionStart,
            mediumPriorityPartitionStart + mediumPriorityPartitionCount - 1
        )
    }

    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster
    ): Int {
        val partitions = cluster.partitionsForTopic(topic)
        val numPartitions = partitions.size

        if (numPartitions == 1) {
            return 0
        }

        val keyString = key?.toString() ?: keyBytes?.let { String(it) } ?: ""
        val priority = detectPriority(keyString)
        val actualKey = removePriorityPrefix(keyString)

        val partition = when (priority) {
            Priority.HIGH -> {
                // 고우선순위는 전용 파티션
                highPriorityPartition.coerceIn(0, numPartitions - 1)
            }
            Priority.MEDIUM -> {
                // 중간 우선순위는 지정된 범위 내에서 해시 분배
                calculateMediumPartition(actualKey, numPartitions)
            }
            Priority.LOW, Priority.DEFAULT -> {
                // 낮은 우선순위는 나머지 파티션에서 해시 분배
                calculateLowPartition(actualKey, numPartitions)
            }
        }

        log.debug(
            "Partitioned message: topic={}, key={}, priority={}, partition={}",
            topic, keyString, priority, partition
        )

        return partition
    }

    private fun detectPriority(key: String): Priority {
        val upperKey = key.uppercase()
        return when {
            HIGH_PRIORITY_PREFIXES.any { upperKey.startsWith(it) } -> Priority.HIGH
            MEDIUM_PRIORITY_PREFIXES.any { upperKey.startsWith(it) } -> Priority.MEDIUM
            LOW_PRIORITY_PREFIXES.any { upperKey.startsWith(it) } -> Priority.LOW
            else -> Priority.DEFAULT
        }
    }

    private fun removePriorityPrefix(key: String): String {
        val allPrefixes = HIGH_PRIORITY_PREFIXES + MEDIUM_PRIORITY_PREFIXES + LOW_PRIORITY_PREFIXES
        for (prefix in allPrefixes) {
            if (key.uppercase().startsWith(prefix)) {
                return key.substring(prefix.length)
            }
        }
        return key
    }

    private fun calculateMediumPartition(key: String, numPartitions: Int): Int {
        val effectiveStart = mediumPriorityPartitionStart.coerceIn(0, numPartitions - 1)
        val effectiveCount = mediumPriorityPartitionCount.coerceAtMost(numPartitions - effectiveStart)

        if (effectiveCount <= 0) {
            return Utils.toPositive(Utils.murmur2(key.toByteArray())) % numPartitions
        }

        val hash = Utils.toPositive(Utils.murmur2(key.toByteArray()))
        return effectiveStart + (hash % effectiveCount)
    }

    private fun calculateLowPartition(key: String, numPartitions: Int): Int {
        // 고우선순위와 중간 우선순위 파티션을 제외한 나머지
        val reservedPartitions = 1 + mediumPriorityPartitionCount // high(1) + medium
        val lowPartitionStart = reservedPartitions.coerceAtMost(numPartitions - 1)
        val lowPartitionCount = (numPartitions - lowPartitionStart).coerceAtLeast(1)

        val hash = Utils.toPositive(Utils.murmur2(key.toByteArray()))
        return lowPartitionStart + (hash % lowPartitionCount)
    }

    override fun close() {
        log.info("PriorityPartitioner closed")
    }

    enum class Priority {
        HIGH, MEDIUM, LOW, DEFAULT
    }
}