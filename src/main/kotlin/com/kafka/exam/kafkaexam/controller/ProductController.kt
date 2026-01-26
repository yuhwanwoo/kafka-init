package com.kafka.exam.kafkaexam.controller

import com.kafka.exam.kafkaexam.service.ProductService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/products")
class ProductController(
    private val productService: ProductService
) {

    @PostMapping
    fun registerProduct(@RequestBody request: ProductRegisterRequest): ProductRegisterResponse {
        productService.registerProduct(request)

        return ProductRegisterResponse(
            productId = request.productId,
            message = "상품 등록 이벤트 발행 완료"
        )
    }
}

data class ProductRegisterRequest(
    val productId: String,
    val name: String,
    val price: Long,
    val category: String
)

data class ProductRegisterResponse(
    val productId: String,
    val message: String
)
