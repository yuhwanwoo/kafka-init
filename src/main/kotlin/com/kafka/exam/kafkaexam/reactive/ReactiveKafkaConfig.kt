package com.kafka.exam.kafkaexam.reactive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import java.time.Duration
import java.util.*

@Configuration
class ReactiveKafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String
) {

    companion object {
        const val REACTIVE_TOPIC = "reactive-events"
    }

    @Bean
    fun reactiveProducerOptions(): SenderOptions<String, String> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5,
            ProducerConfig.LINGER_MS_CONFIG to 5,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384
        )
        return SenderOptions.create<String, String>(props)
            .maxInFlight(1024)
            .stopOnError(false)
    }

    @Bean
    fun reactiveKafkaSender(options: SenderOptions<String, String>): KafkaSender<String, String> {
        return KafkaSender.create(options)
    }

    @Bean
    fun reactiveConsumerOptions(): ReceiverOptions<String, String> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$groupId-reactive",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100,
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300000
        )
        return ReceiverOptions.create<String, String>(props)
            .commitInterval(Duration.ofSeconds(5))
            .commitBatchSize(100)
            .subscription(Collections.singleton(REACTIVE_TOPIC))
    }

    @Bean
    fun reactiveKafkaReceiver(options: ReceiverOptions<String, String>): KafkaReceiver<String, String> {
        return KafkaReceiver.create(options)
    }
}