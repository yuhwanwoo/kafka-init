package com.kafka.exam.kafkaexam.schemaregistry

import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

@Service
class AvroProducer(
    private val avroKafkaTemplate: KafkaTemplate<String, SpecificRecord>
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Value("\${kafka.avro.topic}")
    private lateinit var defaultTopic: String

    fun <T : SpecificRecord> send(message: T): CompletableFuture<SendResult<String, SpecificRecord>> {
        return send(defaultTopic, message)
    }

    fun <T : SpecificRecord> send(topic: String, message: T): CompletableFuture<SendResult<String, SpecificRecord>> {
        log.info("Sending Avro message to topic={}, schema={}", topic, message.schema.name)

        return avroKafkaTemplate.send(topic, message)
            .whenComplete { result, ex ->
                if (ex != null) {
                    log.error("Failed to send Avro message: {}", ex.message, ex)
                } else {
                    val metadata = result.recordMetadata
                    log.info(
                        "Avro message sent successfully: topic={}, partition={}, offset={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                    )
                }
            }
    }

    fun <T : SpecificRecord> send(topic: String, key: String, message: T): CompletableFuture<SendResult<String, SpecificRecord>> {
        log.info("Sending Avro message to topic={}, key={}, schema={}", topic, key, message.schema.name)

        return avroKafkaTemplate.send(topic, key, message)
            .whenComplete { result, ex ->
                if (ex != null) {
                    log.error("Failed to send Avro message with key={}: {}", key, ex.message, ex)
                } else {
                    val metadata = result.recordMetadata
                    log.info(
                        "Avro message sent successfully: topic={}, partition={}, offset={}, key={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        key
                    )
                }
            }
    }

    fun <T : SpecificRecord> sendSync(topic: String, key: String, message: T): SendResult<String, SpecificRecord> {
        log.info("Sending Avro message synchronously: topic={}, key={}", topic, key)
        return avroKafkaTemplate.send(topic, key, message).get()
    }
}