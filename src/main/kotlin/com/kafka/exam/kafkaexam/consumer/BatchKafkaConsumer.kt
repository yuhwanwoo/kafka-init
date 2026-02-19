package com.kafka.exam.kafkaexam.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class BatchKafkaConsumer(
    private val idempotencyRepository: IdempotencyRepository
) {

    private val log = LoggerFactory.getLogger(BatchKafkaConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.batch.topic:batch-topic}"],
        groupId = "\${spring.kafka.consumer.group-id}-batch",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    fun consumeBatch(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment) {
        log.info("배치 메시지 수신 - 총 {}건", records.size)

        val startTime = System.currentTimeMillis()
        var successCount = 0
        var skipCount = 0
        var failCount = 0

        val newRecords = records.filter { record ->
            val messageKey = "${record.topic()}-${record.partition()}-${record.offset()}"
            if (idempotencyRepository.isAlreadyProcessed(messageKey)) {
                skipCount++
                false
            } else {
                true
            }
        }

        for (record in newRecords) {
            try {
                processMessage(record)
                val messageKey = "${record.topic()}-${record.partition()}-${record.offset()}"
                idempotencyRepository.markAsProcessed(messageKey)
                successCount++
            } catch (e: Exception) {
                log.error("메시지 처리 실패 - partition: {}, offset: {}, error: {}",
                    record.partition(), record.offset(), e.message)
                failCount++
            }
        }

        val elapsedTime = System.currentTimeMillis() - startTime
        log.info(
            "배치 처리 완료 - 총: {}, 성공: {}, 스킵(중복): {}, 실패: {}, 소요시간: {}ms",
            records.size, successCount, skipCount, failCount, elapsedTime
        )

        ack.acknowledge()
    }

    private fun processMessage(record: ConsumerRecord<String, String>) {
        val payload = record.value()
        if (payload.isNullOrBlank()) {
            throw IllegalArgumentException("메시지 payload가 비어있습니다. key=${record.key()}")
        }

        // 실제 비즈니스 로직 처리
        log.debug("메시지 처리 - partition: {}, offset: {}, key: {}",
            record.partition(), record.offset(), record.key())
    }
}
