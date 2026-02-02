package com.kafka.exam.kafkaexam.consumer

import org.springframework.stereotype.Repository
import java.util.concurrent.ConcurrentHashMap

@Repository
class IdempotencyRepository {

    private val processedKeys = ConcurrentHashMap<String, Boolean>()

    fun isAlreadyProcessed(messageKey: String): Boolean {
        return processedKeys.containsKey(messageKey)
    }

    fun markAsProcessed(messageKey: String) {
        processedKeys[messageKey] = true
    }
}