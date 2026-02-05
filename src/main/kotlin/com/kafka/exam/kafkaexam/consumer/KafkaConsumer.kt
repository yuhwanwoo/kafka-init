package com.kafka.exam.kafkaexam.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
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
            "Received message - topic: {}, partition: {}, key: {}, value: {}",
            record.topic(),
            record.partition(),
            record.key(),
            record.value()
        )

        processMessage(record)
        idempotencyRepository.markAsProcessed(messageKey)
        ack.acknowledge()
    }

    @DltHandler
    fun handleDlt(
        record: ConsumerRecord<String, String>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE, required = false) exceptionMessage: String?,
        ack: Acknowledgment
    ) {
        log.error(
            "DLT 메시지 수신 - topic: {}, partition: {}, offset: {}, key: {}, value: {}, error: {}",
            topic,
            record.partition(),
            record.offset(),
            record.key(),
            record.value(),
            exceptionMessage ?: "Unknown error"
        )

        // DLT 메시지 처리 로직 (알림, DB 저장 등)
        // TODO: 실패 메시지 저장 또는 알림 발송

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