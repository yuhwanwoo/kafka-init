package com.kafka.exam.kafkaexam.exactlyonce

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

/**
 * Consume-Transform-Produce 패턴 (Exactly-Once Semantics 핵심)
 *
 * 동작 방식:
 * 1. eos-input 토픽에서 메시지를 consume
 * 2. 메시지를 변환(transform)
 * 3. eos-output 토픽으로 produce
 * 4. 1~3이 하나의 트랜잭션으로 묶임 (consumer offset 커밋 포함)
 *
 * 트랜잭션이 실패하면 produce와 offset 커밋이 모두 롤백되어
 * 메시지가 정확히 한 번만 처리됩니다.
 */
@Service
class ExactlyOnceProcessor(
    @Qualifier("eosKafkaTemplate")
    private val eosKafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {

    private val log = LoggerFactory.getLogger(ExactlyOnceProcessor::class.java)

    companion object {
        const val INPUT_TOPIC = "eos-input"
        const val OUTPUT_TOPIC = "eos-output"
        const val AUDIT_TOPIC = "eos-audit"
    }

    /**
     * Consume-Transform-Produce: 단일 출력
     *
     * eosKafkaListenerContainerFactory에 트랜잭션 매니저가 설정되어 있으므로
     * 이 리스너 내의 모든 produce + consumer offset 커밋이 하나의 트랜잭션으로 묶입니다.
     */
    @KafkaListener(
        topics = [INPUT_TOPIC],
        containerFactory = "eosKafkaListenerContainerFactory"
    )
    fun consumeTransformProduce(record: ConsumerRecord<String, String>) {
        log.info(
            "[EOS] 입력 수신 - topic: {}, partition: {}, offset: {}, key: {}, value: {}",
            record.topic(), record.partition(), record.offset(), record.key(), record.value()
        )

        // Transform: 입력 메시지를 가공
        val transformed = transform(record.key(), record.value())

        // Produce: 변환된 메시지를 출력 토픽으로 전송
        // 트랜잭션 컨텍스트 안에서 실행되므로 트랜잭션 실패 시 롤백됨
        eosKafkaTemplate.send(OUTPUT_TOPIC, record.key(), transformed)

        log.info("[EOS] 변환 완료 - key: {}, output: {}", record.key(), transformed)
    }

    /**
     * Consume-Transform-Produce: 다중 출력 (fan-out)
     *
     * 하나의 입력 메시지를 여러 토픽으로 전송하는 패턴.
     * 모든 전송이 하나의 트랜잭션으로 묶여 atomic하게 처리됩니다.
     */
    @KafkaListener(
        topics = ["eos-fanout-input"],
        containerFactory = "eosKafkaListenerContainerFactory"
    )
    fun consumeTransformFanOut(record: ConsumerRecord<String, String>) {
        log.info("[EOS Fan-out] 입력 수신 - key: {}, value: {}", record.key(), record.value())

        val transformed = transform(record.key(), record.value())

        // 출력 토픽과 감사(audit) 토픽에 동시에 전송 - 하나의 트랜잭션
        eosKafkaTemplate.send(OUTPUT_TOPIC, record.key(), transformed)
        eosKafkaTemplate.send(AUDIT_TOPIC, record.key(), createAuditMessage(record, transformed))

        log.info("[EOS Fan-out] 변환 및 감사 로그 전송 완료 - key: {}", record.key())
    }

    private fun transform(key: String?, value: String): String {
        return try {
            val node = objectMapper.readTree(value)
            val result = objectMapper.createObjectNode()
            result.put("originalKey", key ?: "null")
            result.set<com.fasterxml.jackson.databind.JsonNode>("data", node)
            result.put("processedAt", System.currentTimeMillis())
            result.put("processor", "exactly-once")
            objectMapper.writeValueAsString(result)
        } catch (e: Exception) {
            // JSON이 아닌 경우 단순 래핑
            """{"originalKey":"${key ?: "null"}","data":"$value","processedAt":${System.currentTimeMillis()},"processor":"exactly-once"}"""
        }
    }

    private fun createAuditMessage(record: ConsumerRecord<String, String>, transformed: String): String {
        val audit = mapOf(
            "sourceTopicPartition" to "${record.topic()}-${record.partition()}",
            "sourceOffset" to record.offset(),
            "key" to (record.key() ?: "null"),
            "inputSize" to record.value().length,
            "outputSize" to transformed.length,
            "timestamp" to System.currentTimeMillis()
        )
        return objectMapper.writeValueAsString(audit)
    }
}