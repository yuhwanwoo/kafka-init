package com.kafka.exam.kafkaexam.partitioner

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@Configuration
class PartitionerConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${partitioner.vip.customers:}") private val vipCustomers: String,
    @Value("\${partitioner.vip.partition:0}") private val vipPartition: Int
) {

    /**
     * 비즈니스 키 기반 파티셔너를 사용하는 ProducerFactory
     * ORDER-, PAYMENT-, INVENTORY- 접두어에 따라 파티션 할당
     */
    @Bean
    fun businessKeyProducerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.PARTITIONER_CLASS_CONFIG to BusinessKeyPartitioner::class.java.name,
            // 파티션 범위 설정
            BusinessKeyPartitioner.ORDER_PARTITION_START to "0",
            BusinessKeyPartitioner.ORDER_PARTITION_COUNT to "3",
            BusinessKeyPartitioner.PAYMENT_PARTITION_START to "3",
            BusinessKeyPartitioner.PAYMENT_PARTITION_COUNT to "3",
            BusinessKeyPartitioner.INVENTORY_PARTITION_START to "6",
            BusinessKeyPartitioner.INVENTORY_PARTITION_COUNT to "3"
        )
        return DefaultKafkaProducerFactory(config, StringSerializer(), StringSerializer())
    }

    @Bean
    fun businessKeyKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(businessKeyProducerFactory())
    }

    /**
     * 우선순위 기반 파티셔너를 사용하는 ProducerFactory
     * HIGH:, MEDIUM:, LOW: 접두어에 따라 파티션 할당
     */
    @Bean
    fun priorityProducerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.PARTITIONER_CLASS_CONFIG to PriorityPartitioner::class.java.name,
            PriorityPartitioner.HIGH_PRIORITY_PARTITION to "0",
            PriorityPartitioner.MEDIUM_PRIORITY_PARTITION_START to "1",
            PriorityPartitioner.MEDIUM_PRIORITY_PARTITION_COUNT to "2"
        )
        return DefaultKafkaProducerFactory(config, StringSerializer(), StringSerializer())
    }

    @Bean
    fun priorityKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(priorityProducerFactory())
    }

    /**
     * 고객 기반 파티셔너를 사용하는 ProducerFactory
     * VIP 고객은 전용 파티션, 일반 고객은 해시 분배
     */
    @Bean
    fun customerProducerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.PARTITIONER_CLASS_CONFIG to CustomerPartitioner::class.java.name,
            CustomerPartitioner.VIP_CUSTOMERS to vipCustomers,
            CustomerPartitioner.VIP_PARTITION to vipPartition.toString()
        )
        return DefaultKafkaProducerFactory(config, StringSerializer(), StringSerializer())
    }

    @Bean
    fun customerKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(customerProducerFactory())
    }
}