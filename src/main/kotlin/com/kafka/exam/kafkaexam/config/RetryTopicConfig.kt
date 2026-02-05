package com.kafka.exam.kafkaexam.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.RetryTopicConfiguration
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder

@Configuration
class RetryTopicConfig(
    @Value("\${kafka.topic:test-topic}") private val topic: String,
    @Value("\${kafka.retry.attempts:3}") private val attempts: Int,
    @Value("\${kafka.retry.backoff.delay:1000}") private val delay: Long,
    @Value("\${kafka.retry.backoff.multiplier:2}") private val multiplier: Int,
    @Value("\${kafka.retry.backoff.max-delay:10000}") private val maxDelay: Long
) {

    @Bean
    fun retryTopicConfiguration(kafkaTemplate: KafkaTemplate<String, String>): RetryTopicConfiguration {
        return RetryTopicConfigurationBuilder
            .newInstance()
            .exponentialBackoff(delay, multiplier.toDouble(), maxDelay)
            .maxAttempts(attempts)
            .includeTopics(listOf(topic))
            .dltSuffix(".DLT")
            .retryTopicSuffix("-retry")
            .create(kafkaTemplate)
    }
}