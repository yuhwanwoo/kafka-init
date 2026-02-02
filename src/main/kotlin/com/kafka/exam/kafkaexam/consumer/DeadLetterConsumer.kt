package com.kafka.exam.kafkaexam.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class DeadLetterConsumer {

    private val log = LoggerFactory.getLogger(DeadLetterConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.topic:test-topic}.DLT"],
        groupId = "\${spring.kafka.consumer.group-id}-dlt"
    )
    fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        log.error(
            "[DLT] 처리 실패 메시지 수신 - topic: {}, partition: {}, offset: {}, key: {}, value: {}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value()
        )

        // 실패 메시지에 대한 후속 처리
        // ex) DB 저장, 알림 발송, 수동 재처리 큐 등
        log.error("[DLT] 실패 메시지 기록 완료 - key: {}", record.key())

        ack.acknowledge()
    }
}