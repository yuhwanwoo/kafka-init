package com.kafka.exam.kafkaexam.partitioner

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils
import org.slf4j.LoggerFactory

/**
 * 고객 ID 기반 Custom Partitioner
 *
 * 동일 고객의 모든 메시지가 같은 파티션으로 전송되어 순서 보장
 * VIP 고객은 전용 파티션에서 우선 처리
 */
class CustomerPartitioner : Partitioner {

    private val log = LoggerFactory.getLogger(javaClass)

    private val vipCustomers = mutableSetOf<String>()
    private var vipPartition = 0

    companion object {
        const val VIP_CUSTOMERS = "partitioner.vip.customers"
        const val VIP_PARTITION = "partitioner.vip.partition"
    }

    override fun configure(configs: MutableMap<String, *>) {
        // VIP 고객 목록 로드 (쉼표로 구분)
        configs[VIP_CUSTOMERS]?.toString()?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.let { vipCustomers.addAll(it) }

        vipPartition = configs[VIP_PARTITION]?.toString()?.toIntOrNull() ?: 0

        log.info("CustomerPartitioner configured: vipCustomers={}, vipPartition={}", vipCustomers, vipPartition)
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
            return Utils.toPositive(Utils.murmur2(valueBytes ?: ByteArray(0))) % numPartitions
        }

        val customerId = extractCustomerId(key?.toString() ?: String(keyBytes))

        // VIP 고객 체크
        if (customerId != null && vipCustomers.contains(customerId)) {
            val partition = vipPartition.coerceIn(0, numPartitions - 1)
            log.debug("VIP customer detected: customerId={}, partition={}", customerId, partition)
            return partition
        }

        // 일반 고객은 해시 기반 파티셔닝 (VIP 파티션 제외)
        val hash = Utils.toPositive(Utils.murmur2(keyBytes))
        val availablePartitions = numPartitions - 1
        val partition = if (availablePartitions > 0) {
            1 + (hash % availablePartitions)  // 파티션 0(VIP)을 제외
        } else {
            hash % numPartitions
        }

        log.debug("Customer partitioned: customerId={}, partition={}", customerId, partition)
        return partition
    }

    private fun extractCustomerId(key: String): String? {
        // 키 형식: "CUSTOMER-{id}" 또는 "CUST-{id}" 또는 순수 ID
        return when {
            key.startsWith("CUSTOMER-", ignoreCase = true) -> key.substringAfter("-")
            key.startsWith("CUST-", ignoreCase = true) -> key.substringAfter("-")
            key.matches(Regex("^[A-Za-z0-9_-]+$")) -> key
            else -> null
        }
    }

    fun addVipCustomer(customerId: String) {
        vipCustomers.add(customerId)
        log.info("VIP customer added: {}", customerId)
    }

    fun removeVipCustomer(customerId: String) {
        vipCustomers.remove(customerId)
        log.info("VIP customer removed: {}", customerId)
    }

    override fun close() {
        log.info("CustomerPartitioner closed")
    }
}