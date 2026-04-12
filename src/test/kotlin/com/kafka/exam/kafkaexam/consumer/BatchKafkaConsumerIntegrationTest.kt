package com.kafka.exam.kafkaexam.consumer

import com.kafka.exam.kafkaexam.idempotent.ProcessedMessageRepository
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
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
    private lateinit var processedMessageRepository: ProcessedMessageRepository

    private val topic = "test-batch-topic"

    @Test
    fun `배치 메시지 수신 테스트`() {
        val messageCount = 10
        val beforeCount = processedMessageRepository.count()

        repeat(messageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "batch-key-$i", "batch-message-$i")).get()
        }

        await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted {
                val afterCount = processedMessageRepository.count()
                assertTrue(
                    afterCount - beforeCount >= messageCount,
                    "최소 $messageCount 개의 메시지가 배치 처리되어야 합니다"
                )
            }
    }

    @Test
    fun `대량 배치 메시지 처리 테스트`() {
        val messageCount = 50
        val beforeCount = processedMessageRepository.count()

        repeat(messageCount) { i ->
            kafkaTemplate.send(ProducerRecord(topic, "bulk-key-$i", "bulk-message-$i"))
        }
        kafkaTemplate.flush()

        await()
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted {
                val afterCount = processedMessageRepository.count()
                assertTrue(
                    afterCount - beforeCount >= messageCount,
                    "최소 $messageCount 개의 대량 메시지가 처리되어야 합니다"
                )
            }
    }
}