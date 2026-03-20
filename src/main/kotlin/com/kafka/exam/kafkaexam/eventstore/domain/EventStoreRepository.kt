package com.kafka.exam.kafkaexam.eventstore.domain

import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository

@Repository
interface EventStoreRepository : JpaRepository<EventStore, String> {

    fun findByAggregateTypeAndAggregateIdOrderByVersionAsc(
        aggregateType: String,
        aggregateId: String
    ): List<EventStore>

    fun findBySagaIdOrderByTimestampAsc(sagaId: String): List<EventStore>

    fun findByAggregateTypeAndAggregateIdAndVersionGreaterThanOrderByVersionAsc(
        aggregateType: String,
        aggregateId: String,
        version: Long
    ): List<EventStore>

    @Query("SELECT MAX(e.version) FROM EventStore e WHERE e.aggregateType = :aggregateType AND e.aggregateId = :aggregateId")
    fun findMaxVersion(aggregateType: String, aggregateId: String): Long?

    fun findByEventType(eventType: String, pageable: Pageable): Page<EventStore>

    fun countByAggregateTypeAndAggregateId(aggregateType: String, aggregateId: String): Long
}
