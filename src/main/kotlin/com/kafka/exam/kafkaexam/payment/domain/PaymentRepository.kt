package com.kafka.exam.kafkaexam.payment.domain

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface PaymentRepository : JpaRepository<Payment, String> {
    fun findBySagaId(sagaId: String): Payment?
    fun findByOrderId(orderId: String): Payment?
    fun findByStatus(status: PaymentStatus): List<Payment>
}
