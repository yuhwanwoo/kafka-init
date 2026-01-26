package com.kafka.exam.kafkaexam.service

import com.kafka.exam.kafkaexam.controller.ProductRegisterRequest
import com.kafka.exam.kafkaexam.producer.KafkaProducer
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

@Service
class ProductService(
    private val kafkaProducer: KafkaProducer,
    private val objectMapper: ObjectMapper
) {

    fun registerProduct(request: ProductRegisterRequest) {
        val message = objectMapper.writeValueAsString(request)

        // 상품 ID를 파티션 키로 사용 -> 같은 상품은 항상 같은 파티션으로
        kafkaProducer.send("product-topic", request.productId, message)
    }
}