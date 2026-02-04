package com.kafka.exam.kafkaexam.domain

import jakarta.persistence.Entity
import jakarta.persistence.Id

@Entity
class Product(
    @Id
    val productId: String,
    var name: String,
    var price: Long,
    var category: String
)