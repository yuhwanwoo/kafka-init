package com.kafka.exam.kafkaexam.producer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = ["producer-test-topic", "transaction-test-topic"]
)
class KafkaProducerIntegrationTest {

    @Autowired
    private lateinit var kafkaProducer: KafkaProducer

    @Autowired
    private lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    @Test
    fun `메시지 전송 테스트`() {
        val topic = "producer-test-topic"
        val message = "Hello Kafka!"

        kafkaProducer.send(topic, message)

        val consumer = createConsumer("send-test-group")
        embeddedKafkaBroker.consumeFromEmbeddedTopics(consumer, topic)

        val records: ConsumerRecords<String, String> = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))

        assertTrue(records.count() > 0, "메시지가 수신되어야 합니다")
        assertEquals(message, records.first().value())

        consumer.close()
    }

    @Test
    fun `키와 함께 메시지 전송 테스트`() {
        val topic = "producer-test-topic"
        val key = "test-key"
        val message = "Hello with key!"

        kafkaProducer.send(topic, key, message)

        val consumer = createConsumer("send-with-key-test-group")
        embeddedKafkaBroker.consumeFromEmbeddedTopics(consumer, topic)

        val records: ConsumerRecords<String, String> = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))

        val record = records.find { it.key() == key }
        assertTrue(record != null, "지정된 키를 가진 메시지가 수신되어야 합니다")
        assertEquals(key, record.key())
        assertEquals(message, record.value())

        consumer.close()
    }

    @Test
    fun `트랜잭션 메시지 전송 테스트`() {
        val topic = "transaction-test-topic"
        val key = "tx-key"
        val message = "Transactional message"

        kafkaProducer.sendInTransaction(topic, key, message)

        val consumer = createConsumer("transaction-test-group")
        embeddedKafkaBroker.consumeFromEmbeddedTopics(consumer, topic)

        val records: ConsumerRecords<String, String> = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))

        val record = records.find { it.key() == key }
        assertTrue(record != null, "트랜잭션 메시지가 수신되어야 합니다")
        assertEquals(message, record.value())

        consumer.close()
    }

    @Test
    fun `다중 트랜잭션 메시지 전송 테스트`() {
        val topic = "transaction-test-topic"
        val messages = listOf(
            Triple(topic, "key1", "message1"),
            Triple(topic, "key2", "message2"),
            Triple(topic, "key3", "message3")
        )

        kafkaProducer.sendMultipleInTransaction(messages)

        val consumer = createConsumer("multi-transaction-test-group")
        embeddedKafkaBroker.consumeFromEmbeddedTopics(consumer, topic)

        val records: ConsumerRecords<String, String> = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))

        val receivedMessages = records.map { it.key() to it.value() }.toMap()
        assertEquals(3, receivedMessages.size, "3개의 메시지가 수신되어야 합니다")
        assertEquals("message1", receivedMessages["key1"])
        assertEquals("message2", receivedMessages["key2"])
        assertEquals("message3", receivedMessages["key3"])

        consumer.close()
    }

    private fun createConsumer(groupId: String) =
        DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to embeddedKafkaBroker.brokersAsString,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed"
            ),
            StringDeserializer(),
            StringDeserializer()
        ).createConsumer()
}