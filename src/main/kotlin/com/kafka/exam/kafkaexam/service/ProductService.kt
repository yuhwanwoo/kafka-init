package com.kafka.exam.kafkaexam.service

import com.kafka.exam.kafkaexam.controller.dto.request.ProductRegisterRequest
import com.kafka.exam.kafkaexam.controller.dto.request.ProductUpdateRequest
import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

@Service
class ProductService(
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper
) {

    companion object {
        private const val PRODUCT_TOPIC = "product-topic"
    }

    fun registerProduct(request: ProductRegisterRequest) {
        val payload = objectMapper.writeValueAsString(request)

        // 아웃박스 패턴: Kafka로 직접 보내지 않고 아웃박스에 저장
        // 실제 DB 사용 시 비즈니스 로직과 같은 트랜잭션에서 처리됨
        val outbox = Outbox(
            aggregateType = "Product",
            aggregateId = request.productId,
            eventType = OutboxEventType.PRODUCT_REGISTERED,
            payload = payload,
            topic = PRODUCT_TOPIC
        )

        outboxRepository.save(outbox)
    }

    fun updateProduct(productId: String, request: ProductUpdateRequest) {
        val payload = objectMapper.writeValueAsString(
            mapOf(
                "productId" to productId,
                "name" to request.name,
                "price" to request.price,
                "category" to request.category
            )
        )

        val outbox = Outbox(
            aggregateType = "Product",
            aggregateId = productId,
            eventType = OutboxEventType.PRODUCT_UPDATED,
            payload = payload,
            topic = PRODUCT_TOPIC
        )

        outboxRepository.save(outbox)
    }

    fun deleteProduct(productId: String) {
        val payload = objectMapper.writeValueAsString(
            mapOf("productId" to productId)
        )

        val outbox = Outbox(
            aggregateType = "Product",
            aggregateId = productId,
            eventType = OutboxEventType.PRODUCT_DELETED,
            payload = payload,
            topic = PRODUCT_TOPIC
        )

        outboxRepository.save(outbox)
    }
}