package com.kafka.exam.kafkaexam.streams.config

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import java.util.*

@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${kafka.streams.application-id}") private val applicationId: String,
    @Value("\${kafka.streams.state-dir}") private val stateDir: String
) {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        val props = mutableMapOf<String, Any>(
            StreamsConfig.APPLICATION_ID_CONFIG to applicationId,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String()::class.java,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String()::class.java,
            StreamsConfig.STATE_DIR_CONFIG to stateDir,
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE_V2,
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG to 1000,
            StreamsConfig.NUM_STREAM_THREADS_CONFIG to 2
        )
        return KafkaStreamsConfiguration(props)
    }
}