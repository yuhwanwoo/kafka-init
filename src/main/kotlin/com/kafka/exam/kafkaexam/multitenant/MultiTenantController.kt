package com.kafka.exam.kafkaexam.multitenant

import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/tenants")
class MultiTenantController(
    private val tenantRegistry: TenantRegistry,
    private val tenantAwareProducer: TenantAwareProducer,
    private val topicRouter: TenantTopicRouter
) {
    @Value("\${kafka.topic}")
    private lateinit var defaultTopic: String

    // ==================== 테넌트 관리 ====================

    @GetMapping
    fun getAllTenants(): ResponseEntity<List<TenantDto>> {
        val tenants = tenantRegistry.getAllTenants().map { it.toDto() }
        return ResponseEntity.ok(tenants)
    }

    @GetMapping("/active")
    fun getActiveTenants(): ResponseEntity<List<TenantDto>> {
        val tenants = tenantRegistry.getActiveTenants().map { it.toDto() }
        return ResponseEntity.ok(tenants)
    }

    @GetMapping("/{tenantId}")
    fun getTenant(@PathVariable tenantId: String): ResponseEntity<TenantDto> {
        val tenant = tenantRegistry.getTenant(tenantId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(tenant.toDto())
    }

    @PostMapping
    fun createTenant(@RequestBody request: CreateTenantRequest): ResponseEntity<TenantDto> {
        val tenant = Tenant(
            tenantId = request.tenantId,
            tenantName = request.tenantName,
            tier = request.tier ?: TenantTier.BASIC,
            config = TenantConfig(
                maxMessagesPerSecond = request.maxMessagesPerSecond ?: 100,
                maxTopics = request.maxTopics ?: 10,
                retentionDays = request.retentionDays ?: 7
            )
        )

        tenantRegistry.registerTenant(tenant)
        return ResponseEntity.ok(tenant.toDto())
    }

    @PutMapping("/{tenantId}/activate")
    fun activateTenant(@PathVariable tenantId: String): ResponseEntity<Map<String, Any>> {
        tenantRegistry.activateTenant(tenantId)
        return ResponseEntity.ok(mapOf(
            "message" to "Tenant activated",
            "tenantId" to tenantId
        ))
    }

    @PutMapping("/{tenantId}/deactivate")
    fun deactivateTenant(@PathVariable tenantId: String): ResponseEntity<Map<String, Any>> {
        tenantRegistry.deactivateTenant(tenantId)
        return ResponseEntity.ok(mapOf(
            "message" to "Tenant deactivated",
            "tenantId" to tenantId
        ))
    }

    @DeleteMapping("/{tenantId}")
    fun deleteTenant(@PathVariable tenantId: String): ResponseEntity<Map<String, Any>> {
        tenantRegistry.removeTenant(tenantId)
        return ResponseEntity.ok(mapOf(
            "message" to "Tenant removed",
            "tenantId" to tenantId
        ))
    }

    // ==================== 메시지 전송 ====================

    @PostMapping("/{tenantId}/messages")
    fun sendMessage(
        @PathVariable tenantId: String,
        @RequestBody request: SendMessageRequest
    ): ResponseEntity<SendMessageResponse> {
        val topic = request.topic ?: defaultTopic
        val strategy = request.strategy ?: TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER

        val result = tenantAwareProducer.sendForTenant(
            tenantId = tenantId,
            baseTopic = topic,
            key = request.key,
            message = request.message,
            strategy = strategy
        ).get()

        val routedTopic = topicRouter.getTopicForTenant(topic, tenantId, strategy)
        val routedKey = topicRouter.getKeyForTenant(request.key, tenantId, strategy)

        return ResponseEntity.ok(
            SendMessageResponse(
                tenantId = tenantId,
                topic = routedTopic,
                key = routedKey,
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                strategy = strategy.name
            )
        )
    }

    @PostMapping("/broadcast")
    fun broadcastMessage(@RequestBody request: BroadcastRequest): ResponseEntity<BroadcastResponse> {
        val tenantIds = request.tenantIds
            ?: tenantRegistry.getActiveTenants().map { it.tenantId }

        val topic = request.topic ?: defaultTopic
        val strategy = request.strategy ?: TenantTopicStrategy.SHARED_TOPIC_WITH_HEADER

        val results = tenantAwareProducer.broadcast(
            tenantIds = tenantIds,
            baseTopic = topic,
            key = request.key,
            message = request.message,
            strategy = strategy
        )

        // 모든 전송 완료 대기
        val completedResults = results.map { future ->
            try {
                val result = future.get()
                BroadcastResult(
                    topic = result.recordMetadata.topic(),
                    partition = result.recordMetadata.partition(),
                    offset = result.recordMetadata.offset(),
                    success = true
                )
            } catch (e: Exception) {
                BroadcastResult(
                    topic = "",
                    partition = -1,
                    offset = -1,
                    success = false,
                    error = e.message
                )
            }
        }

        return ResponseEntity.ok(
            BroadcastResponse(
                targetTenants = tenantIds.size,
                successCount = completedResults.count { it.success },
                failureCount = completedResults.count { !it.success },
                results = completedResults
            )
        )
    }

    // ==================== 토픽 라우팅 정보 ====================

    @GetMapping("/{tenantId}/topics")
    fun getTenantTopics(@PathVariable tenantId: String): ResponseEntity<TenantTopicsInfo> {
        val tenant = tenantRegistry.getTenant(tenantId)
            ?: return ResponseEntity.notFound().build()

        val baseTopic = defaultTopic
        val strategies = TenantTopicStrategy.entries

        val topicMappings = strategies.associate { strategy ->
            strategy.name to TopicMapping(
                topic = topicRouter.getTopicForTenant(baseTopic, tenantId, strategy),
                keyExample = topicRouter.getKeyForTenant("sample-key", tenantId, strategy)
            )
        }

        return ResponseEntity.ok(
            TenantTopicsInfo(
                tenantId = tenantId,
                tenantName = tenant.tenantName,
                baseTopic = baseTopic,
                topicPattern = topicRouter.getTenantTopicPattern(tenantId),
                consumerGroup = topicRouter.getConsumerGroupForTenant("group", tenantId),
                topicMappings = topicMappings
            )
        )
    }
}

// ==================== DTOs ====================

data class TenantDto(
    val tenantId: String,
    val tenantName: String,
    val tier: TenantTier,
    val isActive: Boolean,
    val config: TenantConfig
)

fun Tenant.toDto() = TenantDto(
    tenantId = tenantId,
    tenantName = tenantName,
    tier = tier,
    isActive = isActive,
    config = config
)

data class CreateTenantRequest(
    val tenantId: String,
    val tenantName: String,
    val tier: TenantTier? = null,
    val maxMessagesPerSecond: Int? = null,
    val maxTopics: Int? = null,
    val retentionDays: Int? = null
)

data class SendMessageRequest(
    val topic: String? = null,
    val key: String,
    val message: Any,
    val strategy: TenantTopicStrategy? = null
)

data class SendMessageResponse(
    val tenantId: String,
    val topic: String,
    val key: String,
    val partition: Int,
    val offset: Long,
    val strategy: String
)

data class BroadcastRequest(
    val tenantIds: List<String>? = null,
    val topic: String? = null,
    val key: String,
    val message: Any,
    val strategy: TenantTopicStrategy? = null
)

data class BroadcastResponse(
    val targetTenants: Int,
    val successCount: Int,
    val failureCount: Int,
    val results: List<BroadcastResult>
)

data class BroadcastResult(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val success: Boolean,
    val error: String? = null
)

data class TenantTopicsInfo(
    val tenantId: String,
    val tenantName: String,
    val baseTopic: String,
    val topicPattern: String,
    val consumerGroup: String,
    val topicMappings: Map<String, TopicMapping>
)

data class TopicMapping(
    val topic: String,
    val keyExample: String
)