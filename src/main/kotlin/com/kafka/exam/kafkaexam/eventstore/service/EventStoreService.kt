package com.kafka.exam.kafkaexam.eventstore.service

import com.kafka.exam.kafkaexam.eventstore.domain.EventStore
import com.kafka.exam.kafkaexam.eventstore.domain.EventStoreRepository
import com.kafka.exam.kafkaexam.saga.event.SagaEvent
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class EventStoreService(
    private val eventStoreRepository: EventStoreRepository,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun storeEvent(
        aggregateType: String,
        aggregateId: String,
        sagaId: String,
        eventType: String,
        event: SagaEvent
    ): EventStore {
        val currentVersion = eventStoreRepository.findMaxVersion(aggregateType, aggregateId) ?: 0L
        val newVersion = currentVersion + 1

        val eventStore = EventStore(
            aggregateType = aggregateType,
            aggregateId = aggregateId,
            sagaId = sagaId,
            eventType = eventType,
            payload = objectMapper.writeValueAsString(event),
            version = newVersion
        )

        val saved = eventStoreRepository.save(eventStore)
        log.info("Stored event: type={}, aggregateId={}, version={}", eventType, aggregateId, newVersion)

        return saved
    }

    @Transactional
    fun storeEventFromPayload(
        aggregateType: String,
        aggregateId: String,
        sagaId: String,
        eventType: String,
        payload: String
    ): EventStore {
        val currentVersion = eventStoreRepository.findMaxVersion(aggregateType, aggregateId) ?: 0L
        val newVersion = currentVersion + 1

        val eventStore = EventStore(
            aggregateType = aggregateType,
            aggregateId = aggregateId,
            sagaId = sagaId,
            eventType = eventType,
            payload = payload,
            version = newVersion
        )

        return eventStoreRepository.save(eventStore)
    }

    fun getEventsByAggregate(aggregateType: String, aggregateId: String): List<EventStore> {
        return eventStoreRepository.findByAggregateTypeAndAggregateIdOrderByVersionAsc(aggregateType, aggregateId)
    }

    fun getEventsBySagaId(sagaId: String): List<EventStore> {
        return eventStoreRepository.findBySagaIdOrderByTimestampAsc(sagaId)
    }

    fun getEventsFromVersion(aggregateType: String, aggregateId: String, fromVersion: Long): List<EventStore> {
        return eventStoreRepository.findByAggregateTypeAndAggregateIdAndVersionGreaterThanOrderByVersionAsc(
            aggregateType, aggregateId, fromVersion
        )
    }

    fun getEventsByType(eventType: String, pageable: Pageable): Page<EventStore> {
        return eventStoreRepository.findByEventType(eventType, pageable)
    }

    fun getEventCount(aggregateType: String, aggregateId: String): Long {
        return eventStoreRepository.countByAggregateTypeAndAggregateId(aggregateType, aggregateId)
    }

    inline fun <reified T : SagaEvent> replayEvents(aggregateType: String, aggregateId: String): List<T> {
        return getEventsByAggregate(aggregateType, aggregateId)
            .mapNotNull { event ->
                try {
                    objectMapper.readValue(event.payload, T::class.java)
                } catch (e: Exception) {
                    log.warn("Failed to deserialize event: {}", event.eventId, e)
                    null
                }
            }
    }
}
