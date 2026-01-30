package com.kafka.exam.kafkaexam

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class KafkaexamApplication

fun main(args: Array<String>) {
    runApplication<KafkaexamApplication>(*args)
}
