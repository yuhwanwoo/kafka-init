package com.kafka.exam.kafkaexam.service

import com.kafka.exam.kafkaexam.controller.dto.request.ProductRegisterRequest
import com.kafka.exam.kafkaexam.controller.dto.request.ProductUpdateRequest
import com.kafka.exam.kafkaexam.domain.Product
import com.kafka.exam.kafkaexam.domain.ProductRepository
import com.kafka.exam.kafkaexam.outbox.Outbox
import com.kafka.exam.kafkaexam.outbox.OutboxEventType
import com.kafka.exam.kafkaexam.outbox.OutboxRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class ProductService(
    private val productRepository: ProductRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper
) {

    companion object {
        private const val PRODUCT_TOPIC = "product-topic"
    }

    @Transactional
    fun registerProduct(request: ProductRegisterRequest) {
        val product = Product(
            productId = request.productId,
            name = request.name,
            price = request.price,
            category = request.category
        )
        productRepository.save(product)

        val payload = objectMapper.writeValueAsString(request)
        val outbox = Outbox(
            aggregateType = "Product",
            aggregateId = request.productId,
            eventType = OutboxEventType.PRODUCT_REGISTERED,
            payload = payload,
            topic = PRODUCT_TOPIC
        )
        outboxRepository.save(outbox)
    }

    @Transactional
    fun updateProduct(productId: String, request: ProductUpdateRequest) {
        val product = productRepository.findById(productId)
            .orElseThrow { IllegalArgumentException("상품을 찾을 수 없습니다. productId=$productId") }

        product.name = request.name
        product.price = request.price
        product.category = request.category

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

    @Transactional
    fun deleteProduct(productId: String) {
        val product = productRepository.findById(productId)
            .orElseThrow { IllegalArgumentException("상품을 찾을 수 없습니다. productId=$productId") }

        productRepository.delete(product)

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