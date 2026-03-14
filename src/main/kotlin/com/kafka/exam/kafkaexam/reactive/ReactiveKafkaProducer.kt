package com.kafka.exam.kafkaexam.reactive

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.time.Duration
import java.util.*

@Component
class ReactiveKafkaProducer(
    private val kafkaSender: KafkaSender<String, String>
) {

    private val log = LoggerFactory.getLogger(ReactiveKafkaProducer::class.java)

    /**
     * 단일 메시지 전송 (논블로킹)
     */
    fun send(topic: String, key: String, value: String): Mono<RecordMetadata> {
        val correlationId = UUID.randomUUID().toString()
        val record = SenderRecord.create(
            ProducerRecord(topic, key, value),
            correlationId
        )

        return kafkaSender.send(Mono.just(record))
            .doOnNext { result ->
                log.info(
                    "Message sent: topic={}, partition={}, offset={}, correlationId={}",
                    result.recordMetadata().topic(),
                    result.recordMetadata().partition(),
                    result.recordMetadata().offset(),
                    result.correlationMetadata()
                )
            }
            .doOnError { error ->
                log.error("Failed to send message: key={}, error={}", key, error.message)
            }
            .map { it.recordMetadata() }
            .single()
    }

    /**
     * 단일 메시지 전송 (키 없음)
     */
    fun send(topic: String, value: String): Mono<RecordMetadata> {
        return send(topic, UUID.randomUUID().toString(), value)
    }

    /**
     * 배치 메시지 전송 (논블로킹, 백프레셔 지원)
     */
    fun sendBatch(topic: String, messages: List<Pair<String, String>>): Flux<SenderResult<String>> {
        val records = Flux.fromIterable(messages)
            .map { (key, value) ->
                SenderRecord.create(
                    ProducerRecord(topic, key, value),
                    key
                )
            }

        return kafkaSender.send(records)
            .doOnNext { result ->
                log.debug(
                    "Batch message sent: partition={}, offset={}, key={}",
                    result.recordMetadata().partition(),
                    result.recordMetadata().offset(),
                    result.correlationMetadata()
                )
            }
            .doOnComplete {
                log.info("Batch send completed: {} messages", messages.size)
            }
            .doOnError { error ->
                log.error("Batch send failed: {}", error.message)
            }
    }

    /**
     * 스트림 데이터 전송 (무한 스트림 지원)
     */
    fun sendStream(topic: String, messageStream: Flux<Pair<String, String>>): Flux<SenderResult<String>> {
        val records = messageStream.map { (key, value) ->
            SenderRecord.create(
                ProducerRecord(topic, key, value),
                key
            )
        }

        return kafkaSender.send(records)
            .doOnNext { result ->
                log.debug(
                    "Stream message sent: offset={}",
                    result.recordMetadata().offset()
                )
            }
    }

    /**
     * 지연 전송 (Delayed Send)
     */
    fun sendDelayed(
        topic: String,
        key: String,
        value: String,
        delay: Duration
    ): Mono<RecordMetadata> {
        return Mono.delay(delay)
            .flatMap { send(topic, key, value) }
            .doOnSubscribe {
                log.info("Scheduled message: key={}, delay={}ms", key, delay.toMillis())
            }
    }

    /**
     * 재시도 포함 전송
     */
    fun sendWithRetry(
        topic: String,
        key: String,
        value: String,
        maxRetries: Long = 3,
        retryDelay: Duration = Duration.ofSeconds(1)
    ): Mono<RecordMetadata> {
        return send(topic, key, value)
            .retryWhen(
                reactor.util.retry.Retry.backoff(maxRetries, retryDelay)
                    .doBeforeRetry { signal ->
                        log.warn(
                            "Retrying send: attempt={}, key={}, error={}",
                            signal.totalRetries() + 1,
                            key,
                            signal.failure().message
                        )
                    }
            )
    }

    /**
     * 헤더 포함 메시지 전송
     */
    fun sendWithHeaders(
        topic: String,
        key: String,
        value: String,
        headers: Map<String, String>
    ): Mono<RecordMetadata> {
        val correlationId = UUID.randomUUID().toString()
        val record = ProducerRecord(topic, key, value)

        headers.forEach { (headerKey, headerValue) ->
            record.headers().add(headerKey, headerValue.toByteArray())
        }

        val senderRecord = SenderRecord.create(record, correlationId)

        return kafkaSender.send(Mono.just(senderRecord))
            .doOnNext { result ->
                log.info(
                    "Message with headers sent: topic={}, headers={}",
                    result.recordMetadata().topic(),
                    headers.keys
                )
            }
            .map { it.recordMetadata() }
            .single()
    }
}