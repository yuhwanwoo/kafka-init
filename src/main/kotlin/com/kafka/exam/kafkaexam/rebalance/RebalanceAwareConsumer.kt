package com.kafka.exam.kafkaexam.rebalance

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

/**
 * Rebalance Listener가 적용된 Consumer 예시
 *
 * rebalanceAwareKafkaListenerContainerFactory를 사용하여
 * 파티션 재할당 시 오프셋 관리 및 상태 정리가 자동으로 수행됨
 */
@Service
class RebalanceAwareConsumer(
    private val consumerStateManager: ConsumerStateManager
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["\${kafka.topic}"],
        containerFactory = "rebalanceAwareKafkaListenerContainerFactory",
        groupId = "\${spring.kafka.consumer.group-id}-rebalance"
    )
    fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val partition = TopicPartition(record.topic(), record.partition())

        try {
            // 메시지 수신 기록
            consumerStateManager.onMessageReceived(partition, record.offset(), record.key())

            log.info(
                "[REBALANCE-CONSUMER] Processing: topic={}, partition={}, offset={}, key={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key()
            )

            // 실제 비즈니스 로직 처리
            processMessage(record)

            // 메시지 처리 완료 기록
            consumerStateManager.onMessageProcessed(partition, record.offset())

            // 수동 ACK
            ack.acknowledge()

        } catch (e: Exception) {
            // 에러 기록
            consumerStateManager.onMessageFailed(partition, record.offset(), e.message ?: "Unknown error")

            log.error(
                "[REBALANCE-CONSUMER] Error processing message: topic={}, partition={}, offset={}, error={}",
                record.topic(),
                record.partition(),
                record.offset(),
                e.message,
                e
            )

            throw e
        }
    }

    private fun processMessage(record: ConsumerRecord<String, String>) {
        // 비즈니스 로직 구현
        log.debug("Processing message: {}", record.value())
    }
}