package com.kafka.exam.kafkaexam.compaction

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal
import java.time.Instant

@RestController
@RequestMapping("/api/state")
class StateStoreController(
    private val stateStoreProducer: StateStoreProducer,
    private val stateStoreConsumer: StateStoreConsumer
) {
    // ==================== User State ====================

    @PostMapping("/users/{userId}")
    fun saveUserState(
        @PathVariable userId: String,
        @RequestBody request: UserStateRequest
    ): ResponseEntity<Map<String, Any>> {
        val state = UserState(
            userId = userId,
            email = request.email,
            name = request.name,
            status = request.status ?: UserStatus.ACTIVE,
            tier = request.tier ?: UserTier.BASIC,
            preferences = request.preferences ?: UserPreferences(),
            lastLoginAt = request.lastLoginAt,
            updatedAt = Instant.now()
        )

        stateStoreProducer.saveUserState(userId, state).get()

        return ResponseEntity.ok(mapOf(
            "message" to "User state saved",
            "userId" to userId,
            "topic" to CompactedTopicConfig.USER_STATE_TOPIC
        ))
    }

    @GetMapping("/users/{userId}")
    fun getUserState(@PathVariable userId: String): ResponseEntity<UserState> {
        val state = stateStoreConsumer.getUserState(userId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(state)
    }

    @GetMapping("/users")
    fun getAllUserStates(): ResponseEntity<Map<String, UserState>> {
        return ResponseEntity.ok(stateStoreConsumer.getAllUserStates())
    }

    @DeleteMapping("/users/{userId}")
    fun deleteUserState(@PathVariable userId: String): ResponseEntity<Map<String, Any>> {
        stateStoreProducer.deleteUserState(userId).get()
        return ResponseEntity.ok(mapOf(
            "message" to "User state deleted (tombstone sent)",
            "userId" to userId
        ))
    }

    // ==================== Product State ====================

    @PostMapping("/products/{productId}")
    fun saveProductState(
        @PathVariable productId: String,
        @RequestBody request: ProductStateRequest
    ): ResponseEntity<Map<String, Any>> {
        val state = ProductState(
            productId = productId,
            name = request.name,
            description = request.description,
            price = request.price,
            originalPrice = request.originalPrice,
            category = request.category,
            status = request.status ?: ProductStatus.AVAILABLE,
            tags = request.tags ?: emptyList(),
            attributes = request.attributes ?: emptyMap(),
            updatedAt = Instant.now()
        )

        stateStoreProducer.saveProductState(productId, state).get()

        return ResponseEntity.ok(mapOf(
            "message" to "Product state saved",
            "productId" to productId,
            "topic" to CompactedTopicConfig.PRODUCT_STATE_TOPIC
        ))
    }

    @GetMapping("/products/{productId}")
    fun getProductState(@PathVariable productId: String): ResponseEntity<ProductState> {
        val state = stateStoreConsumer.getProductState(productId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(state)
    }

    @GetMapping("/products")
    fun getAllProductStates(): ResponseEntity<Map<String, ProductState>> {
        return ResponseEntity.ok(stateStoreConsumer.getAllProductStates())
    }

    @DeleteMapping("/products/{productId}")
    fun deleteProductState(@PathVariable productId: String): ResponseEntity<Map<String, Any>> {
        stateStoreProducer.deleteProductState(productId).get()
        return ResponseEntity.ok(mapOf(
            "message" to "Product state deleted (tombstone sent)",
            "productId" to productId
        ))
    }

    // ==================== Inventory State ====================

    @PostMapping("/inventory/{productId}")
    fun saveInventoryState(
        @PathVariable productId: String,
        @RequestBody request: InventoryStateRequest
    ): ResponseEntity<Map<String, Any>> {
        val state = InventoryState(
            productId = productId,
            totalQuantity = request.totalQuantity,
            availableQuantity = request.availableQuantity,
            reservedQuantity = request.reservedQuantity ?: 0,
            warehouseId = request.warehouseId,
            reorderPoint = request.reorderPoint ?: 10,
            reorderQuantity = request.reorderQuantity ?: 100,
            lastRestockedAt = request.lastRestockedAt,
            updatedAt = Instant.now()
        )

        stateStoreProducer.saveInventoryState(productId, state).get()

        return ResponseEntity.ok(mapOf(
            "message" to "Inventory state saved",
            "productId" to productId,
            "topic" to CompactedTopicConfig.INVENTORY_STATE_TOPIC,
            "isLowStock" to state.isLowStock,
            "isOutOfStock" to state.isOutOfStock
        ))
    }

    @GetMapping("/inventory/{productId}")
    fun getInventoryState(@PathVariable productId: String): ResponseEntity<InventoryState> {
        val state = stateStoreConsumer.getInventoryState(productId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(state)
    }

    @GetMapping("/inventory")
    fun getAllInventoryStates(): ResponseEntity<Map<String, InventoryState>> {
        return ResponseEntity.ok(stateStoreConsumer.getAllInventoryStates())
    }

    @GetMapping("/inventory/low-stock")
    fun getLowStockInventories(): ResponseEntity<List<InventoryState>> {
        val lowStock = stateStoreConsumer.getAllInventoryStates()
            .values
            .filter { it.isLowStock }
        return ResponseEntity.ok(lowStock)
    }

    // ==================== Config State ====================

    @PostMapping("/config/{configKey}")
    fun saveConfigState(
        @PathVariable configKey: String,
        @RequestBody request: ConfigStateRequest
    ): ResponseEntity<Map<String, Any>> {
        val state = ConfigState(
            configKey = configKey,
            configValue = request.configValue,
            valueType = request.valueType ?: ConfigValueType.STRING,
            description = request.description,
            category = request.category ?: "general",
            isEncrypted = request.isEncrypted ?: false,
            updatedBy = request.updatedBy,
            updatedAt = Instant.now()
        )

        stateStoreProducer.saveConfigState(configKey, state).get()

        return ResponseEntity.ok(mapOf(
            "message" to "Config state saved",
            "configKey" to configKey,
            "topic" to CompactedTopicConfig.CONFIG_STATE_TOPIC
        ))
    }

    @GetMapping("/config/{configKey}")
    fun getConfigState(@PathVariable configKey: String): ResponseEntity<ConfigState> {
        val state = stateStoreConsumer.getConfigState(configKey)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(state)
    }

    @GetMapping("/config")
    fun getAllConfigStates(): ResponseEntity<Map<String, ConfigState>> {
        return ResponseEntity.ok(stateStoreConsumer.getAllConfigStates())
    }

    @DeleteMapping("/config/{configKey}")
    fun deleteConfigState(@PathVariable configKey: String): ResponseEntity<Map<String, Any>> {
        stateStoreProducer.deleteConfigState(configKey).get()
        return ResponseEntity.ok(mapOf(
            "message" to "Config state deleted (tombstone sent)",
            "configKey" to configKey
        ))
    }

    // ==================== Stats ====================

    @GetMapping("/stats")
    fun getStats(): ResponseEntity<StateStoreStats> {
        return ResponseEntity.ok(stateStoreConsumer.getStoreStats())
    }
}

// ==================== Request DTOs ====================

data class UserStateRequest(
    val email: String,
    val name: String,
    val status: UserStatus? = null,
    val tier: UserTier? = null,
    val preferences: UserPreferences? = null,
    val lastLoginAt: Instant? = null
)

data class ProductStateRequest(
    val name: String,
    val description: String? = null,
    val price: BigDecimal,
    val originalPrice: BigDecimal? = null,
    val category: String,
    val status: ProductStatus? = null,
    val tags: List<String>? = null,
    val attributes: Map<String, String>? = null
)

data class InventoryStateRequest(
    val totalQuantity: Int,
    val availableQuantity: Int,
    val reservedQuantity: Int? = null,
    val warehouseId: String? = null,
    val reorderPoint: Int? = null,
    val reorderQuantity: Int? = null,
    val lastRestockedAt: Instant? = null
)

data class ConfigStateRequest(
    val configValue: String,
    val valueType: ConfigValueType? = null,
    val description: String? = null,
    val category: String? = null,
    val isEncrypted: Boolean? = null,
    val updatedBy: String? = null
)
