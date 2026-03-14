package com.kafka.exam.kafkaexam.reactive

import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

@RestController
@RequestMapping("/api/reactive")
class ReactiveKafkaController(
    private val producer: ReactiveKafkaProducer,
    private val consumer: ReactiveKafkaConsumer
) {

    companion object {
        private const val DEFAULT_TOPIC = ReactiveKafkaConfig.REACTIVE_TOPIC
    }

    /**
     * 단일 메시지 전송
     * POST /api/reactive/send
     */
    @PostMapping("/send")
    fun sendMessage(@RequestBody request: SendMessageRequest): Mono<SendMessageResponse> {
        return producer.send(
            topic = request.topic ?: DEFAULT_TOPIC,
            key = request.key,
            value = request.value
        ).map { metadata ->
            SendMessageResponse(
                success = true,
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset(),
                timestamp = metadata.timestamp()
            )
        }.onErrorResume { error ->
            Mono.just(
                SendMessageResponse(
                    success = false,
                    error = error.message
                )
            )
        }
    }

    /**
     * 배치 메시지 전송
     * POST /api/reactive/send/batch
     */
    @PostMapping("/send/batch")
    fun sendBatchMessages(@RequestBody request: BatchSendRequest): Mono<BatchSendResponse> {
        val messages = request.messages.map { it.key to it.value }

        return producer.sendBatch(request.topic ?: DEFAULT_TOPIC, messages)
            .collectList()
            .map { results ->
                BatchSendResponse(
                    success = true,
                    sentCount = results.size,
                    results = results.map { result ->
                        SendResult(
                            key = result.correlationMetadata(),
                            partition = result.recordMetadata().partition(),
                            offset = result.recordMetadata().offset()
                        )
                    }
                )
            }
            .onErrorResume { error ->
                Mono.just(
                    BatchSendResponse(
                        success = false,
                        sentCount = 0,
                        error = error.message
                    )
                )
            }
    }

    /**
     * 헤더 포함 메시지 전송
     * POST /api/reactive/send/headers
     */
    @PostMapping("/send/headers")
    fun sendWithHeaders(@RequestBody request: SendWithHeadersRequest): Mono<SendMessageResponse> {
        return producer.sendWithHeaders(
            topic = request.topic ?: DEFAULT_TOPIC,
            key = request.key,
            value = request.value,
            headers = request.headers
        ).map { metadata ->
            SendMessageResponse(
                success = true,
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset(),
                timestamp = metadata.timestamp()
            )
        }.onErrorResume { error ->
            Mono.just(
                SendMessageResponse(
                    success = false,
                    error = error.message
                )
            )
        }
    }

    /**
     * 지연 메시지 전송
     * POST /api/reactive/send/delayed
     */
    @PostMapping("/send/delayed")
    fun sendDelayedMessage(@RequestBody request: DelayedSendRequest): Mono<SendMessageResponse> {
        return producer.sendDelayed(
            topic = request.topic ?: DEFAULT_TOPIC,
            key = request.key,
            value = request.value,
            delay = Duration.ofMillis(request.delayMs)
        ).map { metadata ->
            SendMessageResponse(
                success = true,
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset(),
                timestamp = metadata.timestamp()
            )
        }.onErrorResume { error ->
            Mono.just(
                SendMessageResponse(
                    success = false,
                    error = error.message
                )
            )
        }
    }

    /**
     * SSE (Server-Sent Events) 메시지 스트림
     * GET /api/reactive/stream
     */
    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun messageStream(): Flux<ReactiveMessage> {
        return consumer.messageStream()
    }

    /**
     * 특정 키의 메시지만 스트리밍
     * GET /api/reactive/stream/{key}
     */
    @GetMapping("/stream/{key}", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun messageStreamByKey(@PathVariable key: String): Flux<ReactiveMessage> {
        return consumer.messageStreamByKey(key)
    }

    /**
     * 배치 메시지 스트림 (일정 시간/개수마다 묶어서 전송)
     * GET /api/reactive/stream/batch
     */
    @GetMapping("/stream/batch", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun batchedMessageStream(
        @RequestParam(defaultValue = "10") maxSize: Int,
        @RequestParam(defaultValue = "5000") maxTimeMs: Long
    ): Flux<List<ReactiveMessage>> {
        return consumer.batchedMessageStream(maxSize, Duration.ofMillis(maxTimeMs))
    }

    /**
     * 샘플링된 메시지 스트림
     * GET /api/reactive/stream/sampled
     */
    @GetMapping("/stream/sampled", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun sampledMessageStream(
        @RequestParam(defaultValue = "1000") sampleMs: Long
    ): Flux<ReactiveMessage> {
        return consumer.sampledMessageStream(Duration.ofMillis(sampleMs))
    }

    /**
     * Consumer 메트릭 조회
     * GET /api/reactive/metrics
     */
    @GetMapping("/metrics")
    fun getMetrics(): Mono<ConsumerMetrics> {
        return Mono.just(consumer.getMetrics())
    }

    /**
     * 메트릭 리셋
     * POST /api/reactive/metrics/reset
     */
    @PostMapping("/metrics/reset")
    fun resetMetrics(): Mono<ResponseEntity<String>> {
        consumer.resetMetrics()
        return Mono.just(ResponseEntity.ok("Metrics reset successfully"))
    }

    /**
     * 대량 메시지 생성 (테스트용)
     * POST /api/reactive/generate
     */
    @PostMapping("/generate")
    fun generateMessages(@RequestBody request: GenerateRequest): Flux<SendMessageResponse> {
        return Flux.range(1, request.count)
            .flatMap({ index ->
                producer.send(
                    topic = request.topic ?: DEFAULT_TOPIC,
                    key = "${request.keyPrefix}-$index",
                    value = "${request.valuePrefix}-$index"
                ).map { metadata ->
                    SendMessageResponse(
                        success = true,
                        topic = metadata.topic(),
                        partition = metadata.partition(),
                        offset = metadata.offset()
                    )
                }
            }, request.concurrency)
            .onErrorResume { error ->
                Flux.just(
                    SendMessageResponse(
                        success = false,
                        error = error.message
                    )
                )
            }
    }
}