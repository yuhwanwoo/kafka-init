package com.kafka.exam.kafkaexam.job

import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.outbox.OutboxStatus
import com.kafka.exam.kafkaexam.producer.KafkaProducer
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class OutboxPublishJob(
    private val outboxRepository: OutboxRepository,
    private val kafkaProducer: KafkaProducer
) {

    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 5000)
    fun publishPendingEvents() {
        val pendingEvents = outboxRepository.findPendingEvents()
        if (pendingEvents.isEmpty()) return

        log.info("아웃박스 발행 시작: {}건", pendingEvents.size)

        for (event in pendingEvents) {
            try {
                kafkaProducer.send(event.topic, event.aggregateId, event.payload)
                outboxRepository.updateStatus(event.id, OutboxStatus.SENT)
                log.info("아웃박스 발행 성공: id={}, type={}", event.id, event.eventType)
            } catch (e: Exception) {
                outboxRepository.updateStatus(event.id, OutboxStatus.FAILED)
                log.error("아웃박스 발행 실패: id={}, type={}", event.id, event.eventType, e)
            }
        }
    }
}