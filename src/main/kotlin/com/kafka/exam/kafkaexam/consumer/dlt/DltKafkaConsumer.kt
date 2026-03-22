package com.kafka.exam.kafkaexam.consumer.dlt

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.nio.charset.StandardCharsets

@Component
class DltKafkaConsumer(
    private val failedMessageRepository: FailedMessageRepository
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val EXCEPTION_MESSAGE_HEADER = "kafka_dlt-exception-message"
        private const val EXCEPTION_STACKTRACE_HEADER = "kafka_dlt-exception-stacktrace"
        private const val ORIGINAL_TOPIC_HEADER = "kafka_dlt-original-topic"
        private const val ORIGINAL_PARTITION_HEADER = "kafka_dlt-original-partition"
        private const val ORIGINAL_OFFSET_HEADER = "kafka_dlt-original-offset"
    }

    /**
     * DLT 토픽 메시지 캡처
     * - .DLT 접미사가 붙은 모든 토픽의 메시지를 처리
     */
    @KafkaListener(
        topicPattern = ".*\\.DLT",
        groupId = "dlt-capture-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    fun consumeDltMessage(record: ConsumerRecord<String, String>) {
        log.info("DLT 메시지 수신 - topic: {}, partition: {}, offset: {}",
            record.topic(), record.partition(), record.offset())

        try {
            val originalTopic = extractOriginalTopic(record)
            val errorMessage = extractErrorInfo(record.headers())

            val failedMessage = FailedMessage(
                originalTopic = originalTopic,
                partitionId = extractOriginalPartition(record),
                offsetId = extractOriginalOffset(record),
                messageKey = record.key(),
                messageValue = record.value(),
                errorMessage = errorMessage,
                status = FailedMessageStatus.PENDING
            )

            failedMessageRepository.save(failedMessage)
            log.info("DLT 메시지 저장 완료 - originalTopic: {}, id: {}", originalTopic, failedMessage.id)
        } catch (e: Exception) {
            log.error("DLT 메시지 저장 실패 - topic: {}, error: {}", record.topic(), e.message, e)
        }
    }

    /**
     * Saga Event DLT 토픽 전용 Consumer
     */
    @KafkaListener(
        topics = ["\${kafka.saga.event-topic:saga-event-topic}.DLT"],
        groupId = "saga-dlt-capture-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    fun consumeSagaDltMessage(record: ConsumerRecord<String, String>) {
        log.warn("Saga DLT 메시지 수신 - partition: {}, offset: {}", record.partition(), record.offset())

        val errorMessage = extractErrorInfo(record.headers())
        val originalTopic = getHeaderValue(record.headers(), ORIGINAL_TOPIC_HEADER)
            ?: record.topic().removeSuffix(".DLT")

        val failedMessage = FailedMessage(
            originalTopic = originalTopic,
            partitionId = record.partition(),
            offsetId = record.offset(),
            messageKey = record.key(),
            messageValue = record.value(),
            errorMessage = errorMessage,
            status = FailedMessageStatus.PENDING
        )

        failedMessageRepository.save(failedMessage)
        log.info("Saga DLT 메시지 저장 완료 - id: {}", failedMessage.id)
    }

    private fun extractOriginalTopic(record: ConsumerRecord<String, String>): String {
        return getHeaderValue(record.headers(), ORIGINAL_TOPIC_HEADER)
            ?: record.topic().removeSuffix(".DLT")
    }

    private fun extractOriginalPartition(record: ConsumerRecord<String, String>): Int {
        val partitionStr = getHeaderValue(record.headers(), ORIGINAL_PARTITION_HEADER)
        return partitionStr?.toIntOrNull() ?: record.partition()
    }

    private fun extractOriginalOffset(record: ConsumerRecord<String, String>): Long {
        val offsetStr = getHeaderValue(record.headers(), ORIGINAL_OFFSET_HEADER)
        return offsetStr?.toLongOrNull() ?: record.offset()
    }

    private fun extractErrorInfo(headers: Headers): String {
        val exceptionMessage = getHeaderValue(headers, EXCEPTION_MESSAGE_HEADER)
        val stackTrace = getHeaderValue(headers, EXCEPTION_STACKTRACE_HEADER)

        return buildString {
            if (exceptionMessage != null) {
                append("Exception: $exceptionMessage")
            }
            if (stackTrace != null) {
                append("\n\nStackTrace:\n$stackTrace")
            }
            if (isEmpty()) {
                append("Unknown error")
            }
        }
    }

    private fun getHeaderValue(headers: Headers, key: String): String? {
        return headers.lastHeader(key)?.value()?.let {
            String(it, StandardCharsets.UTF_8)
        }
    }
}
