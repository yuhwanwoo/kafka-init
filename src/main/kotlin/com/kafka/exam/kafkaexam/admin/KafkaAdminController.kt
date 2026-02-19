package com.kafka.exam.kafkaexam.admin

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/admin/kafka")
class KafkaAdminController(
    private val kafkaTopicService: KafkaTopicService
) {

    /**
     * 토픽 목록 조회
     * GET /admin/kafka/topics
     */
    @GetMapping("/topics")
    fun listTopics(): ResponseEntity<List<String>> {
        return ResponseEntity.ok(kafkaTopicService.listTopics())
    }

    /**
     * 토픽 생성
     * POST /admin/kafka/topics
     */
    @PostMapping("/topics")
    fun createTopic(@RequestBody request: CreateTopicRequest): ResponseEntity<Map<String, Any>> {
        val success = kafkaTopicService.createTopic(
            topicName = request.name,
            partitions = request.partitions,
            replicationFactor = request.replicationFactor
        )
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "topic" to request.name))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "토픽 생성 실패"))
        }
    }

    /**
     * 토픽 상세 정보 조회
     * GET /admin/kafka/topics/{topicName}
     */
    @GetMapping("/topics/{topicName}")
    fun describeTopic(@PathVariable topicName: String): ResponseEntity<TopicInfo> {
        val topicInfo = kafkaTopicService.describeTopic(topicName)
        return if (topicInfo != null) {
            ResponseEntity.ok(topicInfo)
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * 토픽 삭제
     * DELETE /admin/kafka/topics/{topicName}
     */
    @DeleteMapping("/topics/{topicName}")
    fun deleteTopic(@PathVariable topicName: String): ResponseEntity<Map<String, Any>> {
        val success = kafkaTopicService.deleteTopic(topicName)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "topic" to topicName))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "토픽 삭제 실패"))
        }
    }

    /**
     * 토픽 존재 여부 확인
     * GET /admin/kafka/topics/{topicName}/exists
     */
    @GetMapping("/topics/{topicName}/exists")
    fun topicExists(@PathVariable topicName: String): ResponseEntity<Map<String, Boolean>> {
        val exists = kafkaTopicService.topicExists(topicName)
        return ResponseEntity.ok(mapOf("exists" to exists))
    }

    /**
     * 토픽 설정 조회
     * GET /admin/kafka/topics/{topicName}/config
     */
    @GetMapping("/topics/{topicName}/config")
    fun getTopicConfig(@PathVariable topicName: String): ResponseEntity<Map<String, String>> {
        return ResponseEntity.ok(kafkaTopicService.getTopicConfig(topicName))
    }

    /**
     * 토픽 설정 변경
     * PUT /admin/kafka/topics/{topicName}/config
     */
    @PutMapping("/topics/{topicName}/config")
    fun updateTopicConfig(
        @PathVariable topicName: String,
        @RequestBody configs: Map<String, String>
    ): ResponseEntity<Map<String, Any>> {
        val success = kafkaTopicService.updateTopicConfig(topicName, configs)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "topic" to topicName))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "토픽 설정 변경 실패"))
        }
    }

    /**
     * 파티션 수 증가
     * PUT /admin/kafka/topics/{topicName}/partitions
     */
    @PutMapping("/topics/{topicName}/partitions")
    fun increasePartitions(
        @PathVariable topicName: String,
        @RequestBody request: IncreasePartitionsRequest
    ): ResponseEntity<Map<String, Any>> {
        val success = kafkaTopicService.increasePartitions(topicName, request.count)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "topic" to topicName, "partitions" to request.count))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "파티션 증가 실패"))
        }
    }

    /**
     * Consumer Group 목록 조회
     * GET /admin/kafka/consumer-groups
     */
    @GetMapping("/consumer-groups")
    fun listConsumerGroups(): ResponseEntity<List<String>> {
        return ResponseEntity.ok(kafkaTopicService.listConsumerGroups())
    }

    /**
     * Consumer Group 상세 정보 조회
     * GET /admin/kafka/consumer-groups/{groupId}
     */
    @GetMapping("/consumer-groups/{groupId}")
    fun describeConsumerGroup(@PathVariable groupId: String): ResponseEntity<ConsumerGroupInfo> {
        val groupInfo = kafkaTopicService.describeConsumerGroup(groupId)
        return if (groupInfo != null) {
            ResponseEntity.ok(groupInfo)
        } else {
            ResponseEntity.notFound().build()
        }
    }
}

data class CreateTopicRequest(
    val name: String,
    val partitions: Int = 3,
    val replicationFactor: Short = 1
)

data class IncreasePartitionsRequest(
    val count: Int
)