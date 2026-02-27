package com.kafka.exam.kafkaexam.outbox

import org.springframework.data.domain.PageRequest
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Repository
@Transactional
class OutboxRepositoryImpl(
    private val jpaOutboxRepository: JpaOutboxRepository
) : OutboxRepository {

    override fun save(outbox: Outbox): Outbox {
        return jpaOutboxRepository.save(outbox)
    }

    override fun findById(id: String): Outbox? {
        return jpaOutboxRepository.findById(id).orElse(null)
    }

    override fun findByStatus(status: OutboxStatus): List<Outbox> {
        return jpaOutboxRepository.findByStatus(status)
    }

    override fun findPendingEvents(limit: Int): List<Outbox> {
        return jpaOutboxRepository.findByStatusOrderByCreatedAtAsc(
            OutboxStatus.PENDING,
            PageRequest.of(0, limit)
        )
    }

    override fun updateStatus(id: String, status: OutboxStatus): Outbox? {
        val outbox = jpaOutboxRepository.findById(id).orElse(null) ?: return null

        outbox.status = status
        if (status == OutboxStatus.SENT) {
            outbox.processedAt = LocalDateTime.now()
        }
        if (status == OutboxStatus.FAILED) {
            outbox.retryCount += 1
        }

        return jpaOutboxRepository.save(outbox)
    }

    override fun delete(id: String) {
        jpaOutboxRepository.deleteById(id)
    }

    override fun deleteAll() {
        jpaOutboxRepository.deleteAll()
    }
}
