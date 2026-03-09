package com.kafka.exam.kafkaexam.partitioner

import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/partitioner")
class PartitionerController(
    private val businessKeyKafkaTemplate: KafkaTemplate<String, String>,
    private val priorityKafkaTemplate: KafkaTemplate<String, String>,
    private val customerKafkaTemplate: KafkaTemplate<String, String>
) {
    @Value("\${kafka.topic}")
    private lateinit var defaultTopic: String

    /**
     * 비즈니스 키 기반 파티셔너 테스트
     *
     * 키 형식:
     * - ORDER-xxx: 주문 파티션 (0-2)
     * - PAYMENT-xxx: 결제 파티션 (3-5)
     * - INVENTORY-xxx: 재고 파티션 (6-8)
     */
    @PostMapping("/business-key")
    fun sendWithBusinessKey(@RequestBody request: PartitionerTestRequest): ResponseEntity<SendResult> {
        val topic = request.topic ?: defaultTopic
        val result = businessKeyKafkaTemplate.send(topic, request.key, request.message).get()

        return ResponseEntity.ok(
            SendResult(
                topic = result.recordMetadata.topic(),
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                key = request.key,
                partitionerType = "BusinessKeyPartitioner"
            )
        )
    }

    /**
     * 우선순위 기반 파티셔너 테스트
     *
     * 키 접두어:
     * - HIGH: 또는 P1: -> 고우선순위 파티션 (0)
     * - MEDIUM: 또는 P2: -> 중간 우선순위 (1-2)
     * - LOW: 또는 P3: -> 낮은 우선순위 (나머지)
     */
    @PostMapping("/priority")
    fun sendWithPriority(@RequestBody request: PartitionerTestRequest): ResponseEntity<SendResult> {
        val topic = request.topic ?: defaultTopic
        val result = priorityKafkaTemplate.send(topic, request.key, request.message).get()

        return ResponseEntity.ok(
            SendResult(
                topic = result.recordMetadata.topic(),
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                key = request.key,
                partitionerType = "PriorityPartitioner"
            )
        )
    }

    /**
     * 고객 기반 파티셔너 테스트
     *
     * 키 형식:
     * - CUSTOMER-xxx 또는 CUST-xxx
     * - VIP 고객은 전용 파티션 (0)
     */
    @PostMapping("/customer")
    fun sendWithCustomer(@RequestBody request: PartitionerTestRequest): ResponseEntity<SendResult> {
        val topic = request.topic ?: defaultTopic
        val result = customerKafkaTemplate.send(topic, request.key, request.message).get()

        return ResponseEntity.ok(
            SendResult(
                topic = result.recordMetadata.topic(),
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                key = request.key,
                partitionerType = "CustomerPartitioner"
            )
        )
    }

    /**
     * 여러 메시지를 각 파티셔너로 전송하여 분배 확인
     */
    @PostMapping("/test-distribution")
    fun testDistribution(@RequestBody request: DistributionTestRequest): ResponseEntity<DistributionResult> {
        val results = mutableListOf<SendResult>()

        // 비즈니스 키 테스트
        listOf("ORDER-001", "ORDER-002", "PAYMENT-001", "INVENTORY-001").forEach { key ->
            val result = businessKeyKafkaTemplate.send(request.topic, key, "test-$key").get()
            results.add(
                SendResult(
                    topic = result.recordMetadata.topic(),
                    partition = result.recordMetadata.partition(),
                    offset = result.recordMetadata.offset(),
                    key = key,
                    partitionerType = "BusinessKeyPartitioner"
                )
            )
        }

        // 우선순위 테스트
        listOf("HIGH:urgent-task", "MEDIUM:normal-task", "LOW:batch-task").forEach { key ->
            val result = priorityKafkaTemplate.send(request.topic, key, "test-$key").get()
            results.add(
                SendResult(
                    topic = result.recordMetadata.topic(),
                    partition = result.recordMetadata.partition(),
                    offset = result.recordMetadata.offset(),
                    key = key,
                    partitionerType = "PriorityPartitioner"
                )
            )
        }

        // 파티션별 분포 계산
        val distribution = results.groupBy { it.partition }
            .mapValues { it.value.size }

        return ResponseEntity.ok(
            DistributionResult(
                totalMessages = results.size,
                results = results,
                partitionDistribution = distribution
            )
        )
    }
}

data class PartitionerTestRequest(
    val topic: String? = null,
    val key: String,
    val message: String
)

data class DistributionTestRequest(
    val topic: String
)

data class SendResult(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: String,
    val partitionerType: String
)

data class DistributionResult(
    val totalMessages: Int,
    val results: List<SendResult>,
    val partitionDistribution: Map<Int, Int>
)