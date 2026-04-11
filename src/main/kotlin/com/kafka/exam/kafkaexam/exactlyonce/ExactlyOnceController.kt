package com.kafka.exam.kafkaexam.exactlyonce

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/eos")
class ExactlyOnceController(
    @Qualifier("eosKafkaTemplate")
    private val eosKafkaTemplate: KafkaTemplate<String, String>,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val log = LoggerFactory.getLogger(ExactlyOnceController::class.java)

    /**
     * EOS 입력 토픽으로 테스트 메시지 전송
     * -> ExactlyOnceProcessor가 consume-transform-produce 처리
     */
    @PostMapping("/send")
    fun sendMessage(@RequestBody request: EosSendRequest): Map<String, Any> {
        kafkaTemplate.send(ExactlyOnceProcessor.INPUT_TOPIC, request.key, request.value)

        log.info("[EOS] 테스트 메시지 전송 - key: {}, value: {}", request.key, request.value)
        return mapOf(
            "status" to "sent",
            "topic" to ExactlyOnceProcessor.INPUT_TOPIC,
            "key" to request.key,
            "description" to "메시지가 eos-input 토픽으로 전송됨 -> ExactlyOnceProcessor가 트랜잭션 내에서 변환 후 eos-output으로 전송"
        )
    }

    /**
     * Fan-out 입력 토픽으로 테스트 메시지 전송
     * -> ExactlyOnceProcessor가 output + audit 토픽으로 동시 전송
     */
    @PostMapping("/send/fanout")
    fun sendFanOutMessage(@RequestBody request: EosSendRequest): Map<String, Any> {
        kafkaTemplate.send("eos-fanout-input", request.key, request.value)

        log.info("[EOS Fan-out] 테스트 메시지 전송 - key: {}, value: {}", request.key, request.value)
        return mapOf(
            "status" to "sent",
            "topic" to "eos-fanout-input",
            "key" to request.key,
            "description" to "메시지가 eos-fanout-input으로 전송됨 -> 트랜잭션 내에서 eos-output + eos-audit 두 토픽으로 atomic 전송"
        )
    }

    /**
     * 트랜잭션으로 다중 메시지를 atomic 전송
     */
    @PostMapping("/send/batch")
    fun sendBatchInTransaction(@RequestBody request: EosBatchRequest): Map<String, Any> {
        eosKafkaTemplate.executeInTransaction { operations ->
            log.info("[EOS] 배치 트랜잭션 시작 - {} 건", request.messages.size)
            request.messages.forEach { msg ->
                operations.send(ExactlyOnceProcessor.INPUT_TOPIC, msg.key, msg.value)
            }
            log.info("[EOS] 배치 트랜잭션 커밋 완료")
        }

        return mapOf(
            "status" to "committed",
            "messageCount" to request.messages.size,
            "description" to "${request.messages.size}건의 메시지가 하나의 트랜잭션으로 atomic 전송됨"
        )
    }

    /**
     * 트랜잭션 롤백 시뮬레이션
     * 3건의 메시지 중 마지막에서 예외 발생 -> 전체 롤백
     */
    @PostMapping("/send/rollback-test")
    fun rollbackTest(@RequestBody request: EosSendRequest): Map<String, Any> {
        return try {
            eosKafkaTemplate.executeInTransaction { operations ->
                operations.send(ExactlyOnceProcessor.INPUT_TOPIC, request.key, request.value)
                operations.send(ExactlyOnceProcessor.INPUT_TOPIC, "${request.key}-2", "${request.value}-second")
                log.info("[EOS] 롤백 테스트 - 예외 발생 직전")
                throw RuntimeException("트랜잭션 롤백 테스트용 예외")
            }
            mapOf("status" to "unexpected-commit")
        } catch (e: Exception) {
            log.warn("[EOS] 트랜잭션 롤백 완료 - 모든 메시지 취소됨: {}", e.message)
            mapOf(
                "status" to "rolled-back",
                "error" to (e.message ?: "unknown"),
                "description" to "2건의 메시지가 전송되었으나 예외로 인해 트랜잭션이 롤백되어 consumer(read_committed)에게 전달되지 않음"
            )
        }
    }
}

data class EosSendRequest(
    val key: String,
    val value: String
)

data class EosBatchRequest(
    val messages: List<EosSendRequest>
)