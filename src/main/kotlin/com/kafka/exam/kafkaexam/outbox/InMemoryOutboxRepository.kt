package com.kafka.exam.kafkaexam.outbox

import org.springframework.stereotype.Repository
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

@Repository
class InMemoryOutboxRepository : OutboxRepository {

    private val store = ConcurrentHashMap<String, Outbox>()

    override fun save(outbox: Outbox): Outbox {
        store[outbox.id] = outbox
        return outbox
    }

    override fun findById(id: String): Outbox? {
        return store[id]
    }

    override fun findByStatus(status: OutboxStatus): List<Outbox> {
        return store.values
            .filter { it.status == status }
            .sortedBy { it.createdAt }
    }

    override fun findPendingEvents(limit: Int): List<Outbox> {
        return store.values
            .filter { it.status == OutboxStatus.PENDING }
            .sortedBy { it.createdAt }
            .take(limit)
    }

    override fun updateStatus(id: String, status: OutboxStatus): Outbox? {
        val existing = store[id] ?: return null
        val updated = existing.copy(
            status = status,
            processedAt = if (status == OutboxStatus.SENT) LocalDateTime.now() else existing.processedAt,
            retryCount = if (status == OutboxStatus.FAILED) existing.retryCount + 1 else existing.retryCount
        )
        store[id] = updated
        return updated
    }

    override fun delete(id: String) {
        store.remove(id)
    }

    override fun deleteAll() {
        store.clear()
    }
}