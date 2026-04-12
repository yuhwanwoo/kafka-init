package com.kafka.exam.kafkaexam.idempotent

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class IdempotentConsumerService(
    private val processedMessageRepository: ProcessedMessageRepository
) {

    private val log = LoggerFactory.getLogger(IdempotentConsumerService::class.java)

    /**
     * 메시지 ID 기반 중복 체크 후 처리를 실행합니다.
     *
     * 전략: DB unique constraint를 활용한 낙관적 중복 방지
     * - 먼저 insert를 시도하고, 이미 존재하면 DataIntegrityViolationException으로 중복 감지
     * - SELECT 후 INSERT 방식보다 race condition에 안전
     *
     * @param record Kafka ConsumerRecord
     * @param messageIdExtractor 메시지에서 고유 ID를 추출하는 함수
     * @param processor 실제 비즈니스 로직
     * @return 처리 여부 (true: 처리됨, false: 중복으로 스킵)
     */
    @Transactional
    fun <K, V> processIdempotently(
        record: ConsumerRecord<K, V>,
        messageIdExtractor: (ConsumerRecord<K, V>) -> String,
        processor: (ConsumerRecord<K, V>) -> Unit
    ): Boolean {
        val messageId = messageIdExtractor(record)

        // 낙관적 방식: insert 먼저 시도
        val entity = ProcessedMessage(
            messageId = messageId,
            topic = record.topic(),
            partitionId = record.partition(),
            offsetId = record.offset(),
            messageKey = record.key()?.toString()
        )

        return try {
            processedMessageRepository.save(entity)
            processedMessageRepository.flush()

            // insert 성공 -> 첫 번째 처리
            processor(record)
            log.info("[멱등성] 메시지 처리 완료 - messageId: {}", messageId)
            true
        } catch (e: DataIntegrityViolationException) {
            // PK 중복 -> 이미 처리된 메시지
            log.info("[멱등성] 중복 메시지 스킵 - messageId: {}", messageId)
            false
        }
    }

    /**
     * topic-partition-offset 기반 메시지 ID 추출 (기본 전략)
     */
    fun <K, V> defaultMessageId(record: ConsumerRecord<K, V>): String {
        return "${record.topic()}-${record.partition()}-${record.offset()}"
    }

    /**
     * 비즈니스 키 기반 메시지 ID 추출
     * 예: 주문 ID, 결제 ID 등 비즈니스 도메인에서 유일한 값
     */
    fun headerBasedMessageId(record: ConsumerRecord<*, *>, headerName: String = "message-id"): String {
        val header = record.headers().lastHeader(headerName)
        if (header != null) {
            return String(header.value())
        }
        // 헤더가 없으면 기본 전략으로 fallback
        return "${record.topic()}-${record.partition()}-${record.offset()}"
    }

    /**
     * 만료된 처리 기록 정리
     */
    @Transactional
    fun cleanupExpired(): Int {
        val deleted = processedMessageRepository.deleteExpired(LocalDateTime.now())
        if (deleted > 0) {
            log.info("[멱등성] 만료된 처리 기록 정리 - {}건 삭제", deleted)
        }
        return deleted
    }

    /**
     * 처리 기록 조회 (디버깅/모니터링용)
     */
    fun getProcessedCount(): Long {
        return processedMessageRepository.count()
    }
}