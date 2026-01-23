package com.kafka.exam.kafkaexam.producer

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    fun send(topic: String, message: String) {
        kafkaTemplate.send(topic, message)
    }

    fun send(topic: String, key: String, message: String) {
        kafkaTemplate.send(topic, key, message)
    }
}