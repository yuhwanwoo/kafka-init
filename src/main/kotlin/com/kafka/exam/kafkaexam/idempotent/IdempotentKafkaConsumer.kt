package com.kafka.exam.kafkaexam.idempotent

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class IdempotentKafkaConsumer(
    private val idempotentConsumerService: IdempotentConsumerService,
    private val objectMapper: ObjectMapper
) {

    private val log = LoggerFactory.getLogger(IdempotentKafkaConsumer::class.java)

    /**
     * 전략 1: topic-partition-offset 기반 멱등성
     *
     * Kafka의 offset을 기반으로 중복 체크.
     * 동일 파티션에서 같은 offset의 메시지는 한 번만 처리됩니다.
     * 리밸런싱 후 재전달되는 메시지를 방지할 때 적합합니다.
     */
    @KafkaListener(
        topics = ["idempotent-offset-topic"],
        groupId = "\${spring.kafka.consumer.group-id}-idempotent"
    )
    fun consumeWithOffsetDedup(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val processed = idempotentConsumerService.processIdempotently(
            record = record,
            messageIdExtractor = { idempotentConsumerService.defaultMessageId(it) },
            processor = { processBusinessLogic(it) }
        )

        if (processed) {
            log.info("[Offset 기반] 메시지 처리 완료 - key: {}", record.key())
        }
        ack.acknowledge()
    }

    /**
     * 전략 2: Kafka Header의 message-id 기반 멱등성
     *
     * Producer가 Kafka Header에 고유 ID를 넣어서 전송한 경우 사용.
     * 동일한 비즈니스 이벤트가 여러 번 발행되어도 한 번만 처리됩니다.
     * 예: Producer 재시도로 인한 중복 발행 방지
     */
    @KafkaListener(
        topics = ["idempotent-header-topic"],
        groupId = "\${spring.kafka.consumer.group-id}-idempotent"
    )
    fun consumeWithHeaderDedup(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val processed = idempotentConsumerService.processIdempotently(
            record = record,
            messageIdExtractor = { idempotentConsumerService.headerBasedMessageId(it) },
            processor = { processBusinessLogic(it) }
        )

        if (processed) {
            log.info("[Header 기반] 메시지 처리 완료 - key: {}", record.key())
        }
        ack.acknowledge()
    }

    /**
     * 전략 3: 비즈니스 키 기반 멱등성
     *
     * 메시지 payload 안의 비즈니스 키(예: orderId)로 중복 체크.
     * 동일한 주문이 여러 번 들어와도 한 번만 처리됩니다.
     */
    @KafkaListener(
        topics = ["idempotent-bizkey-topic"],
        groupId = "\${spring.kafka.consumer.group-id}-idempotent"
    )
    fun consumeWithBusinessKeyDedup(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val processed = idempotentConsumerService.processIdempotently(
            record = record,
            messageIdExtractor = { extractBusinessKey(it) },
            processor = { processBusinessLogic(it) }
        )

        if (processed) {
            log.info("[비즈니스 키 기반] 메시지 처리 완료 - key: {}", record.key())
        }
        ack.acknowledge()
    }

    private fun extractBusinessKey(record: ConsumerRecord<String, String>): String {
        return try {
            val node = objectMapper.readTree(record.value())
            // payload에서 비즈니스 키를 추출 (orderId, paymentId 등)
            val bizKey = node.path("orderId").asText(null)
                ?: node.path("paymentId").asText(null)
                ?: node.path("eventId").asText(null)
            "biz-$bizKey"
        } catch (e: Exception) {
            // JSON 파싱 실패 시 record key 사용
            "biz-${record.key() ?: "${record.partition()}-${record.offset()}"}"
        }
    }

    private fun processBusinessLogic(record: ConsumerRecord<String, String>) {
        log.info(
            "[멱등성 Consumer] 비즈니스 로직 처리 - topic: {}, partition: {}, offset: {}, key: {}, value: {}",
            record.topic(), record.partition(), record.offset(), record.key(), record.value()
        )
    }
}