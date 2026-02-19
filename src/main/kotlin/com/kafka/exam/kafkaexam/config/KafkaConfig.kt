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
import org.springframework.kafka.transaction.KafkaTransactionManager

@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    @Value("\${kafka.transaction.id-prefix:kafka-tx-}") private val transactionIdPrefix: String,
    @Value("\${kafka.batch.max-poll-records:100}") private val maxPollRecords: Int,
    @Value("\${kafka.batch.concurrency:3}") private val batchConcurrency: Int
) {

    private val log = LoggerFactory.getLogger(KafkaConfig::class.java)

    // 일반 Producer (트랜잭션 없음)
    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all"
        )
        return DefaultKafkaProducerFactory(config, StringSerializer(), StringSerializer())
    }

    // 트랜잭션용 Producer
    @Bean
    fun transactionalProducerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,  // 멱등성 필수
            ProducerConfig.RETRIES_CONFIG to Int.MAX_VALUE,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5
        )
        val factory = DefaultKafkaProducerFactory<String, String>(config, StringSerializer(), StringSerializer())
        factory.setTransactionIdPrefix(transactionIdPrefix)
        return factory
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }

    // 트랜잭션용 KafkaTemplate
    @Bean
    fun transactionalKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(transactionalProducerFactory())
    }

    // 트랜잭션 매니저
    @Bean
    fun kafkaTransactionManager(): KafkaTransactionManager<String, String> {
        return KafkaTransactionManager(transactionalProducerFactory())
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed"  // 커밋된 트랜잭션 메시지만 읽기
        )
        return DefaultKafkaConsumerFactory(config, StringDeserializer(), StringDeserializer())
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(consumerFactory())
        factory.setConcurrency(3)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        // @RetryableTopic이 에러 처리를 담당하므로 별도 ErrorHandler 설정 불필요
        return factory
    }

    // 배치 Consumer용 ConsumerFactory
    @Bean
    fun batchConsumerFactory(): ConsumerFactory<String, String> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "$groupId-batch",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords,  // 한 번에 가져올 최대 레코드 수
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 1024,  // 최소 1KB 모일 때까지 대기
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 500   // 최대 500ms 대기
        )
        return DefaultKafkaConsumerFactory(config, StringDeserializer(), StringDeserializer())
    }

    // 배치 리스너 컨테이너 팩토리
    @Bean
    fun batchKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = batchConsumerFactory()
        factory.setConcurrency(batchConcurrency)
        factory.isBatchListener = true  // 배치 모드 활성화
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.containerProperties.isAsyncAcks = true
        return factory
    }
}
