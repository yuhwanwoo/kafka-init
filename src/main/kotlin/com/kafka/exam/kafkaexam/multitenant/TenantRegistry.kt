package com.kafka.exam.kafkaexam.multitenant

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * 테넌트 레지스트리
 *
 * 테넌트 정보 관리 및 검증
 */
@Component
class TenantRegistry {

    private val log = LoggerFactory.getLogger(javaClass)

    private val tenants = ConcurrentHashMap<String, Tenant>()

    init {
        // 기본 테넌트 등록 (실제 운영에서는 DB에서 로드)
        registerTenant(
            Tenant(
                tenantId = "tenant-a",
                tenantName = "Company A",
                tier = TenantTier.PREMIUM,
                config = TenantConfig(
                    maxMessagesPerSecond = 1000,
                    maxTopics = 100,
                    retentionDays = 30
                )
            )
        )

        registerTenant(
            Tenant(
                tenantId = "tenant-b",
                tenantName = "Company B",
                tier = TenantTier.STANDARD,
                config = TenantConfig(
                    maxMessagesPerSecond = 500,
                    maxTopics = 50,
                    retentionDays = 14
                )
            )
        )

        registerTenant(
            Tenant(
                tenantId = "tenant-c",
                tenantName = "Company C",
                tier = TenantTier.BASIC,
                config = TenantConfig(
                    maxMessagesPerSecond = 100,
                    maxTopics = 10,
                    retentionDays = 7
                )
            )
        )
    }

    fun registerTenant(tenant: Tenant) {
        tenants[tenant.tenantId] = tenant
        log.info("Tenant registered: id={}, name={}, tier={}", tenant.tenantId, tenant.tenantName, tenant.tier)
    }

    fun getTenant(tenantId: String): Tenant? {
        return tenants[tenantId]
    }

    fun getAllTenants(): List<Tenant> {
        return tenants.values.toList()
    }

    fun getActiveTenants(): List<Tenant> {
        return tenants.values.filter { it.isActive }
    }

    fun getTenantsByTier(tier: TenantTier): List<Tenant> {
        return tenants.values.filter { it.tier == tier }
    }

    fun updateTenant(tenant: Tenant) {
        if (tenants.containsKey(tenant.tenantId)) {
            tenants[tenant.tenantId] = tenant
            log.info("Tenant updated: id={}", tenant.tenantId)
        }
    }

    fun deactivateTenant(tenantId: String) {
        tenants[tenantId]?.let {
            tenants[tenantId] = it.copy(isActive = false)
            log.info("Tenant deactivated: id={}", tenantId)
        }
    }

    fun activateTenant(tenantId: String) {
        tenants[tenantId]?.let {
            tenants[tenantId] = it.copy(isActive = true)
            log.info("Tenant activated: id={}", tenantId)
        }
    }

    fun removeTenant(tenantId: String) {
        tenants.remove(tenantId)
        log.info("Tenant removed: id={}", tenantId)
    }

    fun isValidTenant(tenantId: String): Boolean {
        return tenants[tenantId]?.isActive == true
    }

    fun getTenantConfig(tenantId: String): TenantConfig? {
        return tenants[tenantId]?.config
    }
}

/**
 * 테넌트 정보
 */
data class Tenant(
    val tenantId: String,
    val tenantName: String,
    val tier: TenantTier = TenantTier.BASIC,
    val isActive: Boolean = true,
    val config: TenantConfig = TenantConfig(),
    val metadata: Map<String, String> = emptyMap(),
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now()
)

/**
 * 테넌트 티어
 */
enum class TenantTier {
    BASIC,      // 기본 (제한된 기능)
    STANDARD,   // 표준
    PREMIUM,    // 프리미엄 (높은 처리량)
    ENTERPRISE  // 엔터프라이즈 (무제한)
}

/**
 * 테넌트별 설정
 */
data class TenantConfig(
    val maxMessagesPerSecond: Int = 100,
    val maxTopics: Int = 10,
    val maxPartitionsPerTopic: Int = 3,
    val retentionDays: Int = 7,
    val compressionEnabled: Boolean = false,
    val encryptionEnabled: Boolean = false,
    val customProperties: Map<String, String> = emptyMap()
)