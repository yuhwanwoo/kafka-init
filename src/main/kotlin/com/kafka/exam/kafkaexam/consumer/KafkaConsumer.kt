package com.kafka.exam.kafkaexam.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class KafkaConsumer(
    private val idempotencyRepository: IdempotencyRepository
) {

    private val log = LoggerFactory.getLogger(KafkaConsumer::class.java)

    @KafkaListener(topics = ["\${kafka.topic:test-topic}"], groupId = "\${spring.kafka.consumer.group-id}")
    fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val messageKey = "${record.topic()}-${record.partition()}-${record.offset()}"

        if (idempotencyRepository.isAlreadyProcessed(messageKey)) {
            log.info("중복 메시지 스킵 - key: {}, messageKey: {}", record.key(), messageKey)
            ack.acknowledge()
            return
        }

        log.info(
            "Received message - partition: {}, key: {}, value: {}",
            record.partition(),
            record.key(),
            record.value()
        )

        processMessage(record)
        idempotencyRepository.markAsProcessed(messageKey)
        ack.acknowledge()
    }

    private fun processMessage(record: ConsumerRecord<String, String>) {
        val payload = record.value()
        if (payload.isNullOrBlank()) {
            throw IllegalArgumentException("메시지 payload가 비어있습니다. key=${record.key()}")
        }

        // 메시지 처리 로직
        log.info("메시지 처리 완료 - key: {}, value: {}", record.key(), payload)
    }
}