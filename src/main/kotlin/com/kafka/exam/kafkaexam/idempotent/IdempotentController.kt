package com.kafka.exam.kafkaexam.idempotent

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import java.util.*

@RestController
@RequestMapping("/idempotent")
class IdempotentController(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val idempotentConsumerService: IdempotentConsumerService
) {

    private val log = LoggerFactory.getLogger(IdempotentController::class.java)

    /**
     * 전략 1 테스트: offset 기반 멱등성
     * 같은 메시지를 여러 번 보내도 각각 다른 offset이므로 모두 처리됨
     */
    @PostMapping("/send/offset")
    fun sendForOffsetDedup(@RequestBody request: IdempotentSendRequest): Map<String, Any> {
        kafkaTemplate.send("idempotent-offset-topic", request.key, request.value)

        return mapOf(
            "status" to "sent",
            "topic" to "idempotent-offset-topic",
            "strategy" to "offset-based",
            "description" to "offset 기반 - 리밸런싱으로 인한 재전달 시 중복 방지"
        )
    }

    /**
     * 전략 2 테스트: header의 message-id 기반 멱등성
     * 같은 message-id로 여러 번 보내면 첫 번째만 처리됨
     */
    @PostMapping("/send/header")
    fun sendForHeaderDedup(@RequestBody request: IdempotentSendRequest): Map<String, Any> {
        val messageId = request.messageId ?: UUID.randomUUID().toString()

        val record = ProducerRecord<String, String>("idempotent-header-topic", request.key, request.value)
        record.headers().add(RecordHeader("message-id", messageId.toByteArray()))
        kafkaTemplate.send(record)

        return mapOf(
            "status" to "sent",
            "topic" to "idempotent-header-topic",
            "messageId" to messageId,
            "strategy" to "header-based",
            "description" to "같은 messageId로 다시 호출하면 중복으로 스킵됨"
        )
    }

    /**
     * 전략 2 중복 테스트: 같은 message-id로 2번 전송
     */
    @PostMapping("/send/header/duplicate-test")
    fun duplicateTest(@RequestBody request: IdempotentSendRequest): Map<String, Any> {
        val messageId = request.messageId ?: UUID.randomUUID().toString()

        repeat(2) {
            val record = ProducerRecord<String, String>("idempotent-header-topic", request.key, request.value)
            record.headers().add(RecordHeader("message-id", messageId.toByteArray()))
            kafkaTemplate.send(record)
        }

        return mapOf(
            "status" to "sent-twice",
            "messageId" to messageId,
            "description" to "같은 messageId로 2번 전송 -> Consumer는 1번만 처리"
        )
    }

    /**
     * 전략 3 테스트: 비즈니스 키(orderId) 기반 멱등성
     */
    @PostMapping("/send/bizkey")
    fun sendForBusinessKeyDedup(@RequestBody request: IdempotentSendRequest): Map<String, Any> {
        kafkaTemplate.send("idempotent-bizkey-topic", request.key, request.value)

        return mapOf(
            "status" to "sent",
            "topic" to "idempotent-bizkey-topic",
            "strategy" to "business-key-based",
            "description" to "payload의 orderId/paymentId/eventId 기반 중복 체크"
        )
    }

    /**
     * 처리 기록 통계
     */
    @GetMapping("/stats")
    fun getStats(): Map<String, Any> {
        return mapOf(
            "processedMessageCount" to idempotentConsumerService.getProcessedCount()
        )
    }

    /**
     * 만료 기록 수동 정리
     */
    @DeleteMapping("/cleanup")
    fun manualCleanup(): Map<String, Any> {
        val deleted = idempotentConsumerService.cleanupExpired()
        return mapOf(
            "deletedCount" to deleted
        )
    }
}

data class IdempotentSendRequest(
    val key: String,
    val value: String,
    val messageId: String? = null
)