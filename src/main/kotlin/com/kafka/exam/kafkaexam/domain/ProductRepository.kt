package com.kafka.exam.kafkaexam.domain

import org.springframework.data.jpa.repository.JpaRepository

interface ProductRepository : JpaRepository<Product, String>