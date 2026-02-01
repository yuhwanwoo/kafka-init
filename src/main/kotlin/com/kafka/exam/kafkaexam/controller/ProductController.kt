package com.kafka.exam.kafkaexam.controller

import com.kafka.exam.kafkaexam.controller.dto.request.ProductRegisterRequest
import com.kafka.exam.kafkaexam.controller.dto.request.ProductUpdateRequest
import com.kafka.exam.kafkaexam.controller.dto.response.ProductRegisterResponse
import com.kafka.exam.kafkaexam.controller.dto.response.ProductDeleteResponse
import com.kafka.exam.kafkaexam.controller.dto.response.ProductUpdateResponse
import com.kafka.exam.kafkaexam.service.ProductService
import org.springframework.web.bind.annotation.*

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

    @PutMapping("/{productId}")
    fun updateProduct(
        @PathVariable productId: String,
        @RequestBody request: ProductUpdateRequest
    ): ProductUpdateResponse {
        productService.updateProduct(productId, request)

        return ProductUpdateResponse(
            productId = productId,
            message = "상품 수정 이벤트 발행 완료"
        )
    }

    @DeleteMapping("/{productId}")
    fun deleteProduct(@PathVariable productId: String): ProductDeleteResponse {
        productService.deleteProduct(productId)

        return ProductDeleteResponse(
            productId = productId,
            message = "상품 삭제 이벤트 발행 완료"
        )
    }
}
