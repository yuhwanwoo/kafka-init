package com.kafka.exam.kafkaexam.partitioner

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils
import org.slf4j.LoggerFactory

/**
 * 비즈니스 키 기반 Custom Partitioner
 *
 * 키 형식에 따라 파티션을 결정:
 * - "ORDER-xxx" -> 주문 전용 파티션 (0~2)
 * - "PAYMENT-xxx" -> 결제 전용 파티션 (3~5)
 * - "INVENTORY-xxx" -> 재고 전용 파티션 (6~8)
 * - 그 외 -> 나머지 파티션에 해시 분배
 */
class BusinessKeyPartitioner : Partitioner {

    private val log = LoggerFactory.getLogger(javaClass)

    private var orderPartitionStart = 0
    private var orderPartitionCount = 3
    private var paymentPartitionStart = 3
    private var paymentPartitionCount = 3
    private var inventoryPartitionStart = 6
    private var inventoryPartitionCount = 3

    companion object {
        const val ORDER_PARTITION_START = "partitioner.order.partition.start"
        const val ORDER_PARTITION_COUNT = "partitioner.order.partition.count"
        const val PAYMENT_PARTITION_START = "partitioner.payment.partition.start"
        const val PAYMENT_PARTITION_COUNT = "partitioner.payment.partition.count"
        const val INVENTORY_PARTITION_START = "partitioner.inventory.partition.start"
        const val INVENTORY_PARTITION_COUNT = "partitioner.inventory.partition.count"
    }

    override fun configure(configs: MutableMap<String, *>) {
        orderPartitionStart = configs[ORDER_PARTITION_START]?.toString()?.toIntOrNull() ?: 0
        orderPartitionCount = configs[ORDER_PARTITION_COUNT]?.toString()?.toIntOrNull() ?: 3
        paymentPartitionStart = configs[PAYMENT_PARTITION_START]?.toString()?.toIntOrNull() ?: 3
        paymentPartitionCount = configs[PAYMENT_PARTITION_COUNT]?.toString()?.toIntOrNull() ?: 3
        inventoryPartitionStart = configs[INVENTORY_PARTITION_START]?.toString()?.toIntOrNull() ?: 6
        inventoryPartitionCount = configs[INVENTORY_PARTITION_COUNT]?.toString()?.toIntOrNull() ?: 3

        log.info(
            "BusinessKeyPartitioner configured: order=[{}-{}], payment=[{}-{}], inventory=[{}-{}]",
            orderPartitionStart, orderPartitionStart + orderPartitionCount - 1,
            paymentPartitionStart, paymentPartitionStart + paymentPartitionCount - 1,
            inventoryPartitionStart, inventoryPartitionStart + inventoryPartitionCount - 1
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

        if (keyBytes == null) {
            // 키가 없으면 라운드로빈
            return Utils.toPositive(Utils.murmur2(valueBytes ?: ByteArray(0))) % numPartitions
        }

        val keyString = key?.toString() ?: String(keyBytes)
        val partition = when {
            keyString.startsWith("ORDER-", ignoreCase = true) -> {
                calculatePartition(keyString, orderPartitionStart, orderPartitionCount, numPartitions)
            }
            keyString.startsWith("PAYMENT-", ignoreCase = true) -> {
                calculatePartition(keyString, paymentPartitionStart, paymentPartitionCount, numPartitions)
            }
            keyString.startsWith("INVENTORY-", ignoreCase = true) ||
            keyString.startsWith("PRODUCT-", ignoreCase = true) -> {
                calculatePartition(keyString, inventoryPartitionStart, inventoryPartitionCount, numPartitions)
            }
            else -> {
                // 기본 해시 파티셔닝
                Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions
            }
        }

        log.debug("Partitioned message: topic={}, key={}, partition={}", topic, keyString, partition)
        return partition
    }

    private fun calculatePartition(
        key: String,
        partitionStart: Int,
        partitionCount: Int,
        totalPartitions: Int
    ): Int {
        // 파티션 범위가 총 파티션 수를 초과하면 조정
        val effectiveStart = partitionStart.coerceAtMost(totalPartitions - 1)
        val effectiveCount = partitionCount.coerceAtMost(totalPartitions - effectiveStart)

        if (effectiveCount <= 0) {
            return Utils.toPositive(Utils.murmur2(key.toByteArray())) % totalPartitions
        }

        val hash = Utils.toPositive(Utils.murmur2(key.toByteArray()))
        return effectiveStart + (hash % effectiveCount)
    }

    override fun close() {
        log.info("BusinessKeyPartitioner closed")
    }
}