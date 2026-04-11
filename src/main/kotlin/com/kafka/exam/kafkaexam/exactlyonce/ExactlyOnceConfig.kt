package com.kafka.exam.kafkaexam.exactlyonce

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
import org.springframework.kafka.transaction.KafkaTransactionManager

@Configuration
class ExactlyOnceConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    @Value("\${kafka.eos.transaction-id-prefix:eos-tx-}") private val eosTransactionIdPrefix: String
) {

    // EOS 전용 ProducerFactory - 멱등성 + 트랜잭션
    @Bean
    fun eosProducerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.RETRIES_CONFIG to Int.MAX_VALUE,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5,
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG to 60000
        )
        val factory = DefaultKafkaProducerFactory<String, String>(config, StringSerializer(), StringSerializer())
        factory.setTransactionIdPrefix(eosTransactionIdPrefix)
        return factory
    }

    @Bean
    fun eosKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(eosProducerFactory())
    }

    @Bean("eosKafkaTransactionManager")
    fun eosKafkaTransactionManager(): KafkaTransactionManager<String, String> {
        return KafkaTransactionManager(eosProducerFactory())
    }

    // EOS Consumer - read_committed + 수동 ACK 비활성화 (트랜잭션 매니저가 offset 커밋 관리)
    @Bean
    fun eosConsumerFactory(): ConsumerFactory<String, String> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$groupId-eos",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed" // 커밋된 트랜잭션 메시지만 읽기
        )
        return DefaultKafkaConsumerFactory(config, StringDeserializer(), StringDeserializer())
    }

    // EOS 리스너 컨테이너 - 트랜잭션 매니저가 offset 커밋을 트랜잭션에 포함
    @Bean
    fun eosKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(eosConsumerFactory())
        factory.containerProperties.eosMode = ContainerProperties.EOSMode.V2
        factory.containerProperties.kafkaAwareTransactionManager = eosKafkaTransactionManager()
        // RECORD 모드: 트랜잭션 매니저가 각 레코드 처리 후 offset을 트랜잭션과 함께 커밋
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
        factory.setConcurrency(1)
        return factory
    }
}