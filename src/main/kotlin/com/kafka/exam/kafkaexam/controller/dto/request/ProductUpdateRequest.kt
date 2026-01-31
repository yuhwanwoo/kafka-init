package com.kafka.exam.kafkaexam.controller.dto.request

data class ProductUpdateRequest(
    val name: String,
    val price: Long,
    val category: String
)