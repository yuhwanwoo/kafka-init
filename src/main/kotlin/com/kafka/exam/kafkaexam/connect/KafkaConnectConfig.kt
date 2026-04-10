package com.kafka.exam.kafkaexam.connect

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestClient

/**
 * Kafka Connect REST API 클라이언트 설정
 */
@Configuration
class KafkaConnectConfig(
    @Value("\${kafka.connect.url:http://localhost:8083}") private val connectUrl: String,
    @Value("\${kafka.connect.timeout-ms:5000}") private val timeoutMs: Int
) {

    @Bean("kafkaConnectRestClient")
    fun kafkaConnectRestClient(): RestClient {
        val factory = SimpleClientHttpRequestFactory().apply {
            setConnectTimeout(timeoutMs)
            setReadTimeout(timeoutMs)
        }

        return RestClient.builder()
            .baseUrl(connectUrl)
            .requestFactory(factory)
            .build()
    }
}