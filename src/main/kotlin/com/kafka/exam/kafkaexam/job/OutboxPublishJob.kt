package com.kafka.exam.kafkaexam.job

import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import com.kafka.exam.kafkaexam.outbox.OutboxStatus
import com.kafka.exam.kafkaexam.producer.KafkaProducer
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class OutboxPublishJob(
    private val outboxRepository: OutboxRepository,
    private val kafkaProducer: KafkaProducer,
    @Qualifier("kafkaCircuitBreaker")
    private val circuitBreaker: CircuitBreaker
) {

    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 5000)
    fun publishPendingEvents() {
        val pendingEvents = outboxRepository.findPendingEvents()
        if (pendingEvents.isEmpty()) return

        // Circuit Breaker가 OPEN 상태면 발행 스킵
        if (circuitBreaker.state == CircuitBreaker.State.OPEN) {
            log.warn("Kafka Circuit Breaker OPEN - 아웃박스 발행 스킵 ({}건 대기 중)", pendingEvents.size)
            return
        }

        log.info("아웃박스 발행 시작: {}건 (Circuit Breaker: {})", pendingEvents.size, circuitBreaker.state)

        for (event in pendingEvents) {
            publishEvent(event)
        }
    }

    private fun publishEvent(event: Outbox) {
        try {
            circuitBreaker.executeRunnable {
                kafkaProducer.send(event.topic, event.aggregateId, event.payload)
            }
            outboxRepository.updateStatus(event.id, OutboxStatus.SENT)
            log.info("아웃박스 발행 성공: id={}, type={}", event.id, event.eventType)
        } catch (e: CallNotPermittedException) {
            log.warn("Circuit Breaker OPEN - 발행 중단: id={}, type={}", event.id, event.eventType)
            // Circuit Breaker가 OPEN되면 나머지 이벤트도 발행하지 않음
        } catch (e: Exception) {
            outboxRepository.updateStatus(event.id, OutboxStatus.FAILED)
            log.error("아웃박스 발행 실패: id={}, type={}, error={}", event.id, event.eventType, e.message)
        }
    }

    fun getCircuitBreakerState(): String {
        return circuitBreaker.state.name
    }

    fun getCircuitBreakerMetrics(): Map<String, Any> {
        val metrics = circuitBreaker.metrics
        return mapOf(
            "state" to circuitBreaker.state.name,
            "failureRate" to metrics.failureRate,
            "numberOfBufferedCalls" to metrics.numberOfBufferedCalls,
            "numberOfFailedCalls" to metrics.numberOfFailedCalls,
            "numberOfSuccessfulCalls" to metrics.numberOfSuccessfulCalls
        )
    }
}