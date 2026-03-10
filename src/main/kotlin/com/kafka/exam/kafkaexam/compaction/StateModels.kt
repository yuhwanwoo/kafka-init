package com.kafka.exam.kafkaexam.compaction

import java.math.BigDecimal
import java.time.Instant

/**
 * 사용자 상태
 * 사용자의 현재 상태를 나타내는 스냅샷
 */
data class UserState(
    val userId: String,
    val email: String,
    val name: String,
    val status: UserStatus = UserStatus.ACTIVE,
    val tier: UserTier = UserTier.BASIC,
    val preferences: UserPreferences = UserPreferences(),
    val lastLoginAt: Instant? = null,
    val updatedAt: Instant = Instant.now()
)

enum class UserStatus {
    ACTIVE, INACTIVE, SUSPENDED, DELETED
}

enum class UserTier {
    BASIC, SILVER, GOLD, PLATINUM, VIP
}

data class UserPreferences(
    val language: String = "ko",
    val timezone: String = "Asia/Seoul",
    val notificationEnabled: Boolean = true,
    val marketingEnabled: Boolean = false
)

/**
 * 상품 상태
 * 상품의 현재 상태 (가격, 상태 등)
 */
data class ProductState(
    val productId: String,
    val name: String,
    val description: String? = null,
    val price: BigDecimal,
    val originalPrice: BigDecimal? = null,
    val category: String,
    val status: ProductStatus = ProductStatus.AVAILABLE,
    val tags: List<String> = emptyList(),
    val attributes: Map<String, String> = emptyMap(),
    val updatedAt: Instant = Instant.now()
)

enum class ProductStatus {
    AVAILABLE, OUT_OF_STOCK, DISCONTINUED, COMING_SOON, HIDDEN
}

/**
 * 재고 상태
 * 상품별 실시간 재고 현황
 */
data class InventoryState(
    val productId: String,
    val totalQuantity: Int,
    val availableQuantity: Int,
    val reservedQuantity: Int = 0,
    val warehouseId: String? = null,
    val reorderPoint: Int = 10,
    val reorderQuantity: Int = 100,
    val lastRestockedAt: Instant? = null,
    val updatedAt: Instant = Instant.now()
) {
    val isLowStock: Boolean
        get() = availableQuantity <= reorderPoint

    val isOutOfStock: Boolean
        get() = availableQuantity <= 0
}

/**
 * 설정 상태
 * 시스템/기능별 설정 값
 */
data class ConfigState(
    val configKey: String,
    val configValue: String,
    val valueType: ConfigValueType = ConfigValueType.STRING,
    val description: String? = null,
    val category: String = "general",
    val isEncrypted: Boolean = false,
    val updatedBy: String? = null,
    val updatedAt: Instant = Instant.now()
)

enum class ConfigValueType {
    STRING, NUMBER, BOOLEAN, JSON, LIST
}

/**
 * 세션 상태 (예시)
 */
data class SessionState(
    val sessionId: String,
    val userId: String,
    val deviceInfo: String? = null,
    val ipAddress: String? = null,
    val createdAt: Instant = Instant.now(),
    val lastActivityAt: Instant = Instant.now(),
    val expiresAt: Instant
) {
    val isExpired: Boolean
        get() = Instant.now().isAfter(expiresAt)
}

/**
 * 캐시 상태 (범용)
 */
data class CacheState<T>(
    val key: String,
    val value: T,
    val ttlSeconds: Long? = null,
    val createdAt: Instant = Instant.now(),
    val expiresAt: Instant? = ttlSeconds?.let { Instant.now().plusSeconds(it) }
) {
    val isExpired: Boolean
        get() = expiresAt?.let { Instant.now().isAfter(it) } ?: false
}