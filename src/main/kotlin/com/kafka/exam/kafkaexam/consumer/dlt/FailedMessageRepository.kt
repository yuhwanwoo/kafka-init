package com.kafka.exam.kafkaexam.consumer.dlt

import org.springframework.data.jpa.repository.JpaRepository

interface FailedMessageRepository : JpaRepository<FailedMessage, Long> {

    fun findByStatus(status: FailedMessageStatus): List<FailedMessage>

    fun findByOriginalTopic(topic: String): List<FailedMessage>

    fun findByOriginalTopicAndStatus(topic: String, status: FailedMessageStatus): List<FailedMessage>

    fun countByStatus(status: FailedMessageStatus): Long
}