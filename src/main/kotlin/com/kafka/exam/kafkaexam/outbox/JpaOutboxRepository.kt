package com.kafka.exam.kafkaexam.outbox

import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository

interface JpaOutboxRepository : JpaRepository<Outbox, String> {

    fun findByStatus(status: OutboxStatus): List<Outbox>

    fun findByStatusOrderByCreatedAtAsc(status: OutboxStatus, pageable: Pageable): List<Outbox>
}