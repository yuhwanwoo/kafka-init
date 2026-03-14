package com.kafka.exam.kafkaexam.reactive

// Request DTOs

data class SendMessageRequest(
    val topic: String? = null,
    val key: String,
    val value: String
)

data class BatchSendRequest(
    val topic: String? = null,
    val messages: List<MessageItem>
)

data class MessageItem(
    val key: String,
    val value: String
)

data class SendWithHeadersRequest(
    val topic: String? = null,
    val key: String,
    val value: String,
    val headers: Map<String, String>
)

data class DelayedSendRequest(
    val topic: String? = null,
    val key: String,
    val value: String,
    val delayMs: Long
)

data class GenerateRequest(
    val topic: String? = null,
    val count: Int = 100,
    val keyPrefix: String = "key",
    val valuePrefix: String = "value",
    val concurrency: Int = 10
)

// Response DTOs

data class SendMessageResponse(
    val success: Boolean,
    val topic: String? = null,
    val partition: Int? = null,
    val offset: Long? = null,
    val timestamp: Long? = null,
    val error: String? = null
)

data class BatchSendResponse(
    val success: Boolean,
    val sentCount: Int,
    val results: List<SendResult>? = null,
    val error: String? = null
)

data class SendResult(
    val key: String,
    val partition: Int,
    val offset: Long
)