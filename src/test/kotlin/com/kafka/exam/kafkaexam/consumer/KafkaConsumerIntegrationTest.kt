package com.kafka.exam.kafkaexam.consumer

import com.kafka.exam.kafkaexam.idempotent.IdempotentConsumerService
import com.kafka.exam.kafkaexam.idempotent.ProcessedMessageRepository
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
    private lateinit var processedMessageRepository: ProcessedMessageRepository

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
                val count = processedMessageRepository.count()
                assertTrue(count > 0, "메시지가 처리되어야 합니다")
            }
    }

    @Test
    fun `중복 메시지 처리 방지 테스트`() {
        val messageId = "test-topic-0-999"

        val exists1 = processedMessageRepository.existsById(messageId)
        assertTrue(!exists1, "아직 처리되지 않은 메시지여야 합니다")

        val entity = com.kafka.exam.kafkaexam.idempotent.ProcessedMessage(
            messageId = messageId,
            topic = "test-topic",
            partitionId = 0,
            offsetId = 999,
            messageKey = null
        )
        processedMessageRepository.save(entity)

        val exists2 = processedMessageRepository.existsById(messageId)
        assertTrue(exists2, "이미 처리된 메시지로 표시되어야 합니다")
    }

    @Test
    fun `여러 메시지 순차 처리 테스트`() {
        val topic = "test-topic"
        val messageCount = 5

        val beforeCount = processedMessageRepository.count()

        repeat(messageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "key-$i", "message-$i")).get()
        }

        await()
            .atMost(15, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted {
                val afterCount = processedMessageRepository.count()
                assertTrue(
                    afterCount - beforeCount >= messageCount,
                    "최소 $messageCount 개의 메시지가 처리되어야 합니다"
                )
            }
    }
}