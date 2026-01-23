package com.kafka.exam.kafkaexam.consumer

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumer {

    private val log = LoggerFactory.getLogger(KafkaConsumer::class.java)

    @KafkaListener(topics = ["\${kafka.topic:test-topic}"], groupId = "\${spring.kafka.consumer.group-id}")
    fun consume(message: String) {
        log.info("Received message: $message")
    }
}