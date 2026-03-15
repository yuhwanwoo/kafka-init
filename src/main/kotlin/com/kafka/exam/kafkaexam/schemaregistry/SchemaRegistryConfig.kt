package com.kafka.exam.kafkaexam.schemaregistry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties

@Configuration
class SchemaRegistryConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${schema.registry.url}")
    private lateinit var schemaRegistryUrl: String

    @Value("\${schema.registry.auto-register-schemas:true}")
    private var autoRegisterSchemas: Boolean = true

    @Value("\${schema.registry.use-latest-version:false}")
    private var useLatestVersion: Boolean = false

    @Value("\${spring.kafka.consumer.group-id}")
    private lateinit var groupId: String

    companion object {
        private const val SCHEMA_CACHE_SIZE = 1000
    }

    @Bean
    fun schemaRegistryClient(): SchemaRegistryClient {
        return CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_CACHE_SIZE)
    }

    @Bean
    fun avroProducerFactory(): ProducerFactory<String, SpecificRecord> {
        val configProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.RETRIES_CONFIG to Int.MAX_VALUE,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5,
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS to autoRegisterSchemas,
            KafkaAvroSerializerConfig.USE_LATEST_VERSION to useLatestVersion
        )
        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun avroKafkaTemplate(): KafkaTemplate<String, SpecificRecord> {
        return KafkaTemplate(avroProducerFactory())
    }

    @Bean
    fun avroConsumerFactory(): ConsumerFactory<String, SpecificRecord> {
        val configProps = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$groupId-avro",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to true
        )
        return DefaultKafkaConsumerFactory(configProps)
    }

    @Bean
    fun avroKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, SpecificRecord> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, SpecificRecord>()
        factory.setConsumerFactory(avroConsumerFactory())
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.setConcurrency(3)
        return factory
    }
}