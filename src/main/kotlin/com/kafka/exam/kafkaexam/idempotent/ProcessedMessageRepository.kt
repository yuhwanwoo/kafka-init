package com.kafka.exam.kafkaexam.idempotent

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.time.LocalDateTime

interface ProcessedMessageRepository : JpaRepository<ProcessedMessage, String> {

    @Modifying
    @Query("DELETE FROM ProcessedMessage p WHERE p.expiredAt < :now")
    fun deleteExpired(now: LocalDateTime): Int
}