package com.kafka.exam.kafkaexam.consumer

import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = ["test-batch-topic"]
)
class BatchKafkaConsumerIntegrationTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var idempotencyRepository: IdempotencyRepository

    private val topic = "test-batch-topic"

    @BeforeEach
    fun setUp() {
        // IdempotencyRepository는 InMemory이므로 별도 초기화 불필요
    }

    @Test
    fun `배치 메시지 수신 테스트`() {
        val messageCount = 10

        repeat(messageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "batch-key-$i", "batch-message-$i")).get()
        }

        await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted {
                var processedCount = 0
                repeat(messageCount + 10) { offset ->
                    if (idempotencyRepository.isAlreadyProcessed("$topic-0-$offset")) {
                        processedCount++
                    }
                }
                assertTrue(processedCount >= messageCount, "최소 $messageCount 개의 메시지가 배치 처리되어야 합니다")
            }
    }

    @Test
    fun `배치 중복 메시지 필터링 테스트`() {
        // 먼저 일부 메시지를 처리된 것으로 표시
        repeat(5) { i ->
            idempotencyRepository.markAsProcessed("$topic-0-pre-$i")
        }

        // 새 메시지 전송
        val newMessageCount = 5
        repeat(newMessageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "new-key-$i", "new-message-$i")).get()
        }

        await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted {
                var newProcessedCount = 0
                repeat(50) { offset ->
                    if (idempotencyRepository.isAlreadyProcessed("$topic-0-$offset")) {
                        newProcessedCount++
                    }
                }
                assertTrue(newProcessedCount >= newMessageCount, "새 메시지들이 처리되어야 합니다")
            }
    }

    @Test
    fun `대량 배치 메시지 처리 테스트`() {
        val messageCount = 50

        repeat(messageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "bulk-key-$i", "bulk-message-$i"))
        }

        // 모든 메시지 전송 완료 대기
        kafkaTemplate.flush()

        await()
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted {
                var processedCount = 0
                repeat(messageCount + 100) { offset ->
                    if (idempotencyRepository.isAlreadyProcessed("$topic-0-$offset")) {
                        processedCount++
                    }
                }
                assertTrue(processedCount >= messageCount, "최소 $messageCount 개의 대량 메시지가 처리되어야 합니다")
            }
    }
}