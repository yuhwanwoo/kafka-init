package com.kafka.exam.kafkaexam.controller.dto.request

data class ProductRegisterRequest(
    val productId: String,
    val name: String,
    val price: Long,
    val category: String
)
