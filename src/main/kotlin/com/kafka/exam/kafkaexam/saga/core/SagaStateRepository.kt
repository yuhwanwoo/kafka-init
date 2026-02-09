package com.kafka.exam.kafkaexam.saga.core

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface SagaStateRepository : JpaRepository<SagaState, String> {
    fun findByOrderId(orderId: String): SagaState?
    fun findByStatus(status: SagaStatus): List<SagaState>
    fun findByStatusIn(statuses: List<SagaStatus>): List<SagaState>
}
