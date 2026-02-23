package com.kafka.exam.kafkaexam.streams.producer

import com.kafka.exam.kafkaexam.streams.model.OrderEvent
import com.kafka.exam.kafkaexam.streams.model.OrderEventType
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.math.BigDecimal
import java.util.*

@Component
class OrderEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    @Value("\${kafka.streams.input-topic}") private val inputTopic: String
) {

    private val log = LoggerFactory.getLogger(OrderEventProducer::class.java)

    fun publishOrderEvent(event: OrderEvent) {
        val json = objectMapper.writeValueAsString(event)
        kafkaTemplate.send(inputTopic, event.productId, json)
            .whenComplete { result, ex ->
                if (ex != null) {
                    log.error("Failed to publish order event: {}", event.orderId, ex)
                } else {
                    log.info("Published order event: orderId={}, topic={}, partition={}",
                        event.orderId, result.recordMetadata.topic(), result.recordMetadata.partition())
                }
            }
    }

    fun publishSampleEvents(count: Int = 10) {
        val products = listOf(
            "PROD-001" to "MacBook Pro",
            "PROD-002" to "iPhone 15",
            "PROD-003" to "AirPods Pro",
            "PROD-004" to "iPad Air",
            "PROD-005" to "Apple Watch"
        )

        repeat(count) { i ->
            val (productId, productName) = products.random()
            val event = OrderEvent(
                orderId = "ORD-${UUID.randomUUID().toString().take(8)}",
                productId = productId,
                productName = productName,
                quantity = (1..5).random(),
                price = BigDecimal((100..2000).random()),
                customerId = "CUST-${(1..100).random()}",
                eventType = if (i % 10 == 9) OrderEventType.CANCELLED else OrderEventType.CREATED
            )
            publishOrderEvent(event)
        }
        log.info("Published {} sample order events", count)
    }
}