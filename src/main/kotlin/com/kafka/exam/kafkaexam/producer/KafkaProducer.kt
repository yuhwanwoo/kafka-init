package com.kafka.exam.kafkaexam.producer

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    @Qualifier("transactionalKafkaTemplate")
    private val transactionalKafkaTemplate: KafkaTemplate<String, String>
) {
    private val log = LoggerFactory.getLogger(KafkaProducer::class.java)

    // 일반 메시지 전송 (트랜잭션 없음)
    fun send(topic: String, message: String) {
        kafkaTemplate.send(topic, message)
    }

    fun send(topic: String, key: String, message: String) {
        kafkaTemplate.send(topic, key, message)
    }

    // 트랜잭션 메시지 전송 (단일 메시지)
    fun sendInTransaction(topic: String, key: String, message: String) {
        transactionalKafkaTemplate.executeInTransaction { operations ->
            log.info("트랜잭션 시작 - topic: {}, key: {}", topic, key)
            operations.send(topic, key, message)
            log.info("트랜잭션 커밋 완료")
        }
    }

    // 트랜잭션 메시지 전송 (다중 메시지 - 모두 성공하거나 모두 실패)
    fun sendMultipleInTransaction(messages: List<Triple<String, String, String>>) {
        transactionalKafkaTemplate.executeInTransaction { operations ->
            log.info("다중 메시지 트랜잭션 시작 - {} 건", messages.size)
            messages.forEach { (topic, key, message) ->
                operations.send(topic, key, message)
            }
            log.info("다중 메시지 트랜잭션 커밋 완료")
        }
    }

    // 커스텀 트랜잭션 실행 (콜백 방식)
    fun <T> executeInTransaction(action: (KafkaOperations<String, String>) -> T): T? {
        return transactionalKafkaTemplate.executeInTransaction { operations ->
            action(operations)
        }
    }
}