package com.kafka.exam.kafkaexam.admin

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

@Service
class KafkaAclService(
    private val kafkaAdmin: KafkaAdmin
) {
    private val log = LoggerFactory.getLogger(KafkaAclService::class.java)
    private val timeoutSeconds = 10L

    private fun getAdminClient(): AdminClient {
        return AdminClient.create(kafkaAdmin.configurationProperties)
    }

    /**
     * ACL 생성
     */
    fun createAcl(request: AclCreateRequest): Boolean {
        return getAdminClient().use { client ->
            try {
                val resourcePattern = ResourcePattern(
                    request.resourceType.toKafkaResourceType(),
                    request.resourceName,
                    request.patternType.toKafkaPatternType()
                )

                val accessControlEntry = AccessControlEntry(
                    request.principal,
                    request.host,
                    request.operation.toKafkaOperation(),
                    request.permissionType.toKafkaPermissionType()
                )

                val aclBinding = AclBinding(resourcePattern, accessControlEntry)

                client.createAcls(listOf(aclBinding))
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                log.info(
                    "ACL 생성 완료 - resource: {}:{}, principal: {}, operation: {}, permission: {}",
                    request.resourceType, request.resourceName,
                    request.principal, request.operation, request.permissionType
                )
                true
            } catch (e: Exception) {
                log.error("ACL 생성 실패 - error: {}", e.message)
                false
            }
        }
    }

    /**
     * 여러 ACL 일괄 생성
     */
    fun createAcls(requests: List<AclCreateRequest>): AclBatchResult {
        return getAdminClient().use { client ->
            try {
                val aclBindings = requests.map { request ->
                    val resourcePattern = ResourcePattern(
                        request.resourceType.toKafkaResourceType(),
                        request.resourceName,
                        request.patternType.toKafkaPatternType()
                    )
                    val accessControlEntry = AccessControlEntry(
                        request.principal,
                        request.host,
                        request.operation.toKafkaOperation(),
                        request.permissionType.toKafkaPermissionType()
                    )
                    AclBinding(resourcePattern, accessControlEntry)
                }

                client.createAcls(aclBindings)
                    .all()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                log.info("ACL 일괄 생성 완료 - count: {}", requests.size)
                AclBatchResult(success = true, count = requests.size)
            } catch (e: Exception) {
                log.error("ACL 일괄 생성 실패 - error: {}", e.message)
                AclBatchResult(success = false, count = 0, error = e.message)
            }
        }
    }

    /**
     * 전체 ACL 목록 조회
     */
    fun listAcls(): List<AclInfo> {
        return getAdminClient().use { client ->
            try {
                val filter = AclBindingFilter.ANY
                val acls = client.describeAcls(filter)
                    .values()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                acls.map { binding ->
                    AclInfo(
                        resourceType = binding.pattern().resourceType().name,
                        resourceName = binding.pattern().name(),
                        patternType = binding.pattern().patternType().name,
                        principal = binding.entry().principal(),
                        host = binding.entry().host(),
                        operation = binding.entry().operation().name,
                        permissionType = binding.entry().permissionType().name
                    )
                }.also {
                    log.info("ACL 목록 조회 완료 - count: {}", it.size)
                }
            } catch (e: Exception) {
                log.error("ACL 목록 조회 실패 - error: {}", e.message)
                emptyList()
            }
        }
    }

    /**
     * 리소스별 ACL 조회
     */
    fun getAclsByResource(resourceType: AclResourceType, resourceName: String): List<AclInfo> {
        return getAdminClient().use { client ->
            try {
                val resourcePattern = ResourcePatternFilter(
                    resourceType.toKafkaResourceType(),
                    resourceName,
                    PatternType.ANY
                )
                val filter = AclBindingFilter(resourcePattern, AccessControlEntryFilter.ANY)

                val acls = client.describeAcls(filter)
                    .values()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                acls.map { binding ->
                    AclInfo(
                        resourceType = binding.pattern().resourceType().name,
                        resourceName = binding.pattern().name(),
                        patternType = binding.pattern().patternType().name,
                        principal = binding.entry().principal(),
                        host = binding.entry().host(),
                        operation = binding.entry().operation().name,
                        permissionType = binding.entry().permissionType().name
                    )
                }.also {
                    log.info("리소스별 ACL 조회 완료 - resourceType: {}, resourceName: {}, count: {}",
                        resourceType, resourceName, it.size)
                }
            } catch (e: Exception) {
                log.error("리소스별 ACL 조회 실패 - error: {}", e.message)
                emptyList()
            }
        }
    }

    /**
     * Principal별 ACL 조회
     */
    fun getAclsByPrincipal(principal: String): List<AclInfo> {
        return getAdminClient().use { client ->
            try {
                val entryFilter = AccessControlEntryFilter(
                    principal,
                    null,
                    AclOperation.ANY,
                    AclPermissionType.ANY
                )
                val filter = AclBindingFilter(ResourcePatternFilter.ANY, entryFilter)

                val acls = client.describeAcls(filter)
                    .values()
                    .get(timeoutSeconds, TimeUnit.SECONDS)

                acls.map { binding ->
                    AclInfo(
                        resourceType = binding.pattern().resourceType().name,
                        resourceName = binding.pattern().name(),
                        patternType = binding.pattern().patternType().name,
                        principal = binding.entry().principal(),
                        host = binding.entry().host(),
                        operation = binding.entry().operation().name,
                        permissionType = binding.entry().permissionType().name
                    )
                }.also {
                    log.info("Principal별 ACL 조회 완료 - principal: {}, count: {}", principal, it.size)
                }
            } catch (e: Exception) {
                log.error("Principal별 ACL 조회 실패 - error: {}", e.message)
                emptyList()
            }
        }
    }

    /**
     * ACL 삭제
     */
    fun deleteAcl(request: AclDeleteRequest): Boolean {
        return getAdminClient().use { client ->
            try {
                val resourcePatternFilter = ResourcePatternFilter(
                    request.resourceType.toKafkaResourceType(),
                    request.resourceName,
                    request.patternType?.toKafkaPatternType() ?: PatternType.ANY
                )

                val entryFilter = AccessControlEntryFilter(
                    request.principal,
                    request.host ?: "*",
                    request.operation?.toKafkaOperation() ?: AclOperation.ANY,
                    request.permissionType?.toKafkaPermissionType() ?: AclPermissionType.ANY
                )

                val filter = AclBindingFilter(resourcePatternFilter, entryFilter)

                val deleteResult = client.deleteAcls(listOf(filter))
                deleteResult.all().get(timeoutSeconds, TimeUnit.SECONDS)

                // 각 필터별 결과에서 삭제된 ACL 수 계산
                val filterResults = deleteResult.values()
                var deletedCount = 0
                for ((_, future) in filterResults) {
                    val filterResult = future.get(timeoutSeconds, TimeUnit.SECONDS)
                    deletedCount += filterResult.values().size
                }
                log.info("ACL 삭제 완료 - count: {}", deletedCount)
                deletedCount > 0
            } catch (e: Exception) {
                log.error("ACL 삭제 실패 - error: {}", e.message)
                false
            }
        }
    }

    /**
     * 리소스의 모든 ACL 삭제
     */
    fun deleteAllAclsForResource(resourceType: AclResourceType, resourceName: String): Int {
        return getAdminClient().use { client ->
            try {
                val resourcePatternFilter = ResourcePatternFilter(
                    resourceType.toKafkaResourceType(),
                    resourceName,
                    PatternType.ANY
                )
                val filter = AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY)

                val deleteResult = client.deleteAcls(listOf(filter))
                deleteResult.all().get(timeoutSeconds, TimeUnit.SECONDS)

                val filterResults = deleteResult.values()
                var deletedCount = 0
                for ((_, future) in filterResults) {
                    val filterResult = future.get(timeoutSeconds, TimeUnit.SECONDS)
                    deletedCount += filterResult.values().size
                }
                log.info("리소스 ACL 전체 삭제 완료 - resourceType: {}, resourceName: {}, count: {}",
                    resourceType, resourceName, deletedCount)
                deletedCount
            } catch (e: Exception) {
                log.error("리소스 ACL 전체 삭제 실패 - error: {}", e.message)
                0
            }
        }
    }

    /**
     * 토픽에 대한 프로듀서 권한 부여 (편의 메서드)
     */
    fun grantProducerPermission(topicName: String, principal: String): Boolean {
        val requests = listOf(
            AclCreateRequest(
                resourceType = AclResourceType.TOPIC,
                resourceName = topicName,
                patternType = AclPatternType.LITERAL,
                principal = principal,
                host = "*",
                operation = AclOperationType.WRITE,
                permissionType = AclPermission.ALLOW
            ),
            AclCreateRequest(
                resourceType = AclResourceType.TOPIC,
                resourceName = topicName,
                patternType = AclPatternType.LITERAL,
                principal = principal,
                host = "*",
                operation = AclOperationType.DESCRIBE,
                permissionType = AclPermission.ALLOW
            )
        )
        return createAcls(requests).success
    }

    /**
     * 토픽에 대한 컨슈머 권한 부여 (편의 메서드)
     */
    fun grantConsumerPermission(topicName: String, groupId: String, principal: String): Boolean {
        val requests = listOf(
            // 토픽 읽기 권한
            AclCreateRequest(
                resourceType = AclResourceType.TOPIC,
                resourceName = topicName,
                patternType = AclPatternType.LITERAL,
                principal = principal,
                host = "*",
                operation = AclOperationType.READ,
                permissionType = AclPermission.ALLOW
            ),
            AclCreateRequest(
                resourceType = AclResourceType.TOPIC,
                resourceName = topicName,
                patternType = AclPatternType.LITERAL,
                principal = principal,
                host = "*",
                operation = AclOperationType.DESCRIBE,
                permissionType = AclPermission.ALLOW
            ),
            // 컨슈머 그룹 권한
            AclCreateRequest(
                resourceType = AclResourceType.GROUP,
                resourceName = groupId,
                patternType = AclPatternType.LITERAL,
                principal = principal,
                host = "*",
                operation = AclOperationType.READ,
                permissionType = AclPermission.ALLOW
            )
        )
        return createAcls(requests).success
    }

    /**
     * 토픽에 대한 관리자 권한 부여 (편의 메서드)
     */
    fun grantAdminPermission(topicName: String, principal: String): Boolean {
        val request = AclCreateRequest(
            resourceType = AclResourceType.TOPIC,
            resourceName = topicName,
            patternType = AclPatternType.LITERAL,
            principal = principal,
            host = "*",
            operation = AclOperationType.ALL,
            permissionType = AclPermission.ALLOW
        )
        return createAcl(request)
    }
}

// DTO 클래스들
data class AclCreateRequest(
    val resourceType: AclResourceType,
    val resourceName: String,
    val patternType: AclPatternType = AclPatternType.LITERAL,
    val principal: String,  // 예: "User:alice", "User:CN=Alice,O=MyOrg"
    val host: String = "*",
    val operation: AclOperationType,
    val permissionType: AclPermission
)

data class AclDeleteRequest(
    val resourceType: AclResourceType,
    val resourceName: String,
    val patternType: AclPatternType? = null,
    val principal: String,
    val host: String? = null,
    val operation: AclOperationType? = null,
    val permissionType: AclPermission? = null
)

data class AclInfo(
    val resourceType: String,
    val resourceName: String,
    val patternType: String,
    val principal: String,
    val host: String,
    val operation: String,
    val permissionType: String
)

data class AclBatchResult(
    val success: Boolean,
    val count: Int,
    val error: String? = null
)

// ACL 관련 Enum
enum class AclResourceType {
    TOPIC,
    GROUP,
    CLUSTER,
    TRANSACTIONAL_ID,
    DELEGATION_TOKEN;

    fun toKafkaResourceType(): ResourceType = when (this) {
        TOPIC -> ResourceType.TOPIC
        GROUP -> ResourceType.GROUP
        CLUSTER -> ResourceType.CLUSTER
        TRANSACTIONAL_ID -> ResourceType.TRANSACTIONAL_ID
        DELEGATION_TOKEN -> ResourceType.DELEGATION_TOKEN
    }
}

enum class AclPatternType {
    LITERAL,
    PREFIXED;

    fun toKafkaPatternType(): PatternType = when (this) {
        LITERAL -> PatternType.LITERAL
        PREFIXED -> PatternType.PREFIXED
    }
}

enum class AclOperationType {
    ALL,
    READ,
    WRITE,
    CREATE,
    DELETE,
    ALTER,
    DESCRIBE,
    CLUSTER_ACTION,
    DESCRIBE_CONFIGS,
    ALTER_CONFIGS,
    IDEMPOTENT_WRITE;

    fun toKafkaOperation(): AclOperation = when (this) {
        ALL -> AclOperation.ALL
        READ -> AclOperation.READ
        WRITE -> AclOperation.WRITE
        CREATE -> AclOperation.CREATE
        DELETE -> AclOperation.DELETE
        ALTER -> AclOperation.ALTER
        DESCRIBE -> AclOperation.DESCRIBE
        CLUSTER_ACTION -> AclOperation.CLUSTER_ACTION
        DESCRIBE_CONFIGS -> AclOperation.DESCRIBE_CONFIGS
        ALTER_CONFIGS -> AclOperation.ALTER_CONFIGS
        IDEMPOTENT_WRITE -> AclOperation.IDEMPOTENT_WRITE
    }
}

enum class AclPermission {
    ALLOW,
    DENY;

    fun toKafkaPermissionType(): AclPermissionType = when (this) {
        ALLOW -> AclPermissionType.ALLOW
        DENY -> AclPermissionType.DENY
    }
}