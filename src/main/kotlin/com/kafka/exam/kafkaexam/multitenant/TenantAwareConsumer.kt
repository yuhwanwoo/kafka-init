package com.kafka.exam.kafkaexam.multitenant

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets

/**
 * 테넌트 인식 Consumer
 */
@Service
class TenantAwareConsumer(
    private val tenantMessageHandler: TenantMessageHandler,
    private val topicRouter: TenantTopicRouter
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 공유 토픽에서 테넌트별 메시지 처리
     */
    @KafkaListener(
        topics = ["\${kafka.topic}"],
        groupId = "\${spring.kafka.consumer.group-id}-multitenant",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeSharedTopic(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val tenantId = extractTenantId(record)

        if (tenantId == null) {
            log.warn(
                "[TENANT-CONSUMER] No tenant ID found: topic={}, key={}",
                record.topic(), record.key()
            )
            ack.acknowledge()
            return
        }

        try {
            // 테넌트 컨텍스트 설정
            TenantContext.setTenant(tenantId)

            log.info(
                "[TENANT-CONSUMER] Processing: tenant={}, topic={}, partition={}, offset={}",
                tenantId, record.topic(), record.partition(), record.offset()
            )

            // 테넌트별 메시지 핸들러 호출
            tenantMessageHandler.handleMessage(tenantId, record)

            ack.acknowledge()

        } catch (e: TenantAccessDeniedException) {
            log.error("[TENANT-CONSUMER] Access denied: tenant={}, error={}", tenantId, e.message)
            ack.acknowledge() // DLT로 보내지 않고 무시
        } catch (e: Exception) {
            log.error(
                "[TENANT-CONSUMER] Error processing: tenant={}, error={}",
                tenantId, e.message, e
            )
            throw e
        } finally {
            TenantContext.clear()
        }
    }

    /**
     * 레코드에서 테넌트 ID 추출
     */
    private fun extractTenantId(record: ConsumerRecord<String, String>): String? {
        // 1. 헤더에서 추출 시도
        val headerValue = record.headers()
            .lastHeader(TenantContext.TENANT_ID_HEADER)
            ?.value()
            ?.let { String(it, StandardCharsets.UTF_8) }

        if (headerValue != null) {
            return headerValue
        }

        // 2. 키에서 추출 시도 (prefix 전략)
        val keyTenant = record.key()?.let { topicRouter.extractTenantFromKey(it) }
        if (keyTenant != null) {
            return keyTenant
        }

        // 3. 토픽 이름에서 추출 시도
        return topicRouter.extractTenantFromTopic(record.topic())
    }
}

/**
 * 테넌트별 메시지 핸들러 인터페이스
 */
interface TenantMessageHandler {
    fun handleMessage(tenantId: String, record: ConsumerRecord<String, String>)
}

/**
 * 기본 테넌트 메시지 핸들러
 */
@Service
class DefaultTenantMessageHandler(
    private val tenantRegistry: TenantRegistry
) : TenantMessageHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun handleMessage(tenantId: String, record: ConsumerRecord<String, String>) {
        // 테넌트 유효성 검사
        val tenant = tenantRegistry.getTenant(tenantId)
        if (tenant == null) {
            log.warn("[HANDLER] Unknown tenant: {}", tenantId)
            return
        }

        if (!tenant.isActive) {
            throw TenantAccessDeniedException(tenantId, "Tenant is not active")
        }

        // 테넌트별 처리 로직
        log.info(
            "[HANDLER] Processing for tenant: id={}, name={}, message={}",
            tenant.tenantId, tenant.tenantName, record.value()?.take(100)
        )

        // 실제 비즈니스 로직은 여기에 구현
        // 예: 테넌트별 DB 스키마 선택, 테넌트별 설정 적용 등
    }
}