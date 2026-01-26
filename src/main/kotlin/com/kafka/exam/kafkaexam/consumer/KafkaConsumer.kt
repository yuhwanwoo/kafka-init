package com.kafka.exam.kafkaexam.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class KafkaConsumer {

    private val log = LoggerFactory.getLogger(KafkaConsumer::class.java)

    @KafkaListener(topics = ["\${kafka.topic:test-topic}"], groupId = "\${spring.kafka.consumer.group-id}")
    fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        log.info(
            "Received message - partition: {}, key: {}, value: {}",
            record.partition(),
            record.key(),
            record.value()
        )
        ack.acknowledge()
    }
}