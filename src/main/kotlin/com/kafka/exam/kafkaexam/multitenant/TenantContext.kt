package com.kafka.exam.kafkaexam.multitenant

import org.slf4j.LoggerFactory
import org.slf4j.MDC

/**
 * 테넌트 컨텍스트 홀더
 *
 * ThreadLocal을 사용하여 현재 스레드의 테넌트 정보 관리
 */
object TenantContext {

    private val log = LoggerFactory.getLogger(TenantContext::class.java)

    private val currentTenant = ThreadLocal<TenantInfo>()

    const val TENANT_ID_HEADER = "X-Tenant-Id"
    const val TENANT_NAME_HEADER = "X-Tenant-Name"
    const val MDC_TENANT_ID = "tenantId"

    fun setTenant(tenantInfo: TenantInfo) {
        currentTenant.set(tenantInfo)
        MDC.put(MDC_TENANT_ID, tenantInfo.tenantId)
        log.debug("Tenant context set: {}", tenantInfo.tenantId)
    }

    fun setTenant(tenantId: String, tenantName: String? = null) {
        setTenant(TenantInfo(tenantId, tenantName ?: tenantId))
    }

    fun getTenant(): TenantInfo? {
        return currentTenant.get()
    }

    fun getTenantId(): String? {
        return currentTenant.get()?.tenantId
    }

    fun requireTenant(): TenantInfo {
        return currentTenant.get()
            ?: throw TenantNotSetException("Tenant context is not set")
    }

    fun requireTenantId(): String {
        return requireTenant().tenantId
    }

    fun clear() {
        val tenant = currentTenant.get()
        currentTenant.remove()
        MDC.remove(MDC_TENANT_ID)
        if (tenant != null) {
            log.debug("Tenant context cleared: {}", tenant.tenantId)
        }
    }

    fun <T> executeWithTenant(tenantId: String, block: () -> T): T {
        val previousTenant = getTenant()
        return try {
            setTenant(tenantId)
            block()
        } finally {
            if (previousTenant != null) {
                setTenant(previousTenant)
            } else {
                clear()
            }
        }
    }
}

/**
 * 테넌트 정보
 */
data class TenantInfo(
    val tenantId: String,
    val tenantName: String,
    val metadata: Map<String, String> = emptyMap()
) {
    companion object {
        fun of(tenantId: String) = TenantInfo(tenantId, tenantId)
    }
}

/**
 * 테넌트 미설정 예외
 */
class TenantNotSetException(message: String) : RuntimeException(message)

/**
 * 테넌트 접근 거부 예외
 */
class TenantAccessDeniedException(
    val tenantId: String,
    message: String = "Access denied for tenant: $tenantId"
) : RuntimeException(message)