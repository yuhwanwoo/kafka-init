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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = ["test-topic"]
)
class KafkaConsumerIntegrationTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var idempotencyRepository: IdempotencyRepository

    companion object {
        val receivedMessages = CopyOnWriteArrayList<String>()
    }

    @BeforeEach
    fun setUp() {
        receivedMessages.clear()
    }

    @Test
    fun `메시지 수신 테스트`() {
        val topic = "test-topic"
        val key = "consumer-test-key"
        val message = "Consumer test message"

        kafkaTemplate.send(ProducerRecord(topic, key, message)).get()

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .untilAsserted {
                assertTrue(
                    idempotencyRepository.isAlreadyProcessed("$topic-0-0") ||
                    idempotencyRepository.isAlreadyProcessed("$topic-0-1"),
                    "메시지가 처리되어야 합니다"
                )
            }
    }

    @Test
    fun `중복 메시지 처리 방지 테스트`() {
        val messageKey = "test-idempotency-key"

        idempotencyRepository.markAsProcessed(messageKey)

        assertTrue(
            idempotencyRepository.isAlreadyProcessed(messageKey),
            "이미 처리된 메시지로 표시되어야 합니다"
        )
    }

    @Test
    fun `여러 메시지 순차 처리 테스트`() {
        val topic = "test-topic"
        val messageCount = 5

        repeat(messageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "key-$i", "message-$i")).get()
        }

        await()
            .atMost(15, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted {
                var processedCount = 0
                repeat(messageCount + 10) { offset ->
                    if (idempotencyRepository.isAlreadyProcessed("$topic-0-$offset")) {
                        processedCount++
                    }
                }
                assertTrue(processedCount >= messageCount, "최소 $messageCount 개의 메시지가 처리되어야 합니다")
            }
    }
}