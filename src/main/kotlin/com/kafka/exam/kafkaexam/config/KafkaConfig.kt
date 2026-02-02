package com.kafka.exam.kafkaexam.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    @Value("\${kafka.dlt.retry-count:3}") private val retryCount: Long,
    @Value("\${kafka.dlt.retry-interval-ms:1000}") private val retryIntervalMs: Long
) {

    private val log = LoggerFactory.getLogger(KafkaConfig::class.java)

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all"
        )
        return DefaultKafkaProducerFactory(config, StringSerializer(), StringSerializer())
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        return DefaultKafkaConsumerFactory(config, StringDeserializer(), StringDeserializer())
    }

    @Bean
    fun deadLetterPublishingRecoverer(): DeadLetterPublishingRecoverer {
        val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate())
        recoverer.setLogLevel(org.springframework.kafka.KafkaException.Level.ERROR)
        return recoverer
    }

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        val recoverer = deadLetterPublishingRecoverer()
        val backOff = FixedBackOff(retryIntervalMs, retryCount)
        val handler = DefaultErrorHandler(recoverer, backOff)
        handler.setRetryListeners { record, ex, deliveryAttempt ->
            log.warn(
                "메시지 처리 재시도 - attempt: {}, topic: {}, key: {}, error: {}",
                deliveryAttempt, record.topic(), record.key(), ex.message
            )
        }
        return handler
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory()
        factory.setConcurrency(3)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.setCommonErrorHandler(errorHandler())
        return factory
    }
}
