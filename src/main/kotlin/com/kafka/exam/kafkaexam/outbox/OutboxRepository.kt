package com.kafka.exam.kafkaexam.outbox

interface OutboxRepository {
    fun save(outbox: Outbox): Outbox
    fun findById(id: String): Outbox?
    fun findByStatus(status: OutboxStatus): List<Outbox>
    fun findPendingEvents(limit: Int = 100): List<Outbox>
    fun updateStatus(id: String, status: OutboxStatus): Outbox?
    fun delete(id: String)
    fun deleteAll()
}