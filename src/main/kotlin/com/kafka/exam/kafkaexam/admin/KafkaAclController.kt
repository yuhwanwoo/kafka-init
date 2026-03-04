package com.kafka.exam.kafkaexam.admin

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/admin/kafka/acls")
class KafkaAclController(
    private val kafkaAclService: KafkaAclService
) {

    /**
     * 전체 ACL 목록 조회
     * GET /admin/kafka/acls
     */
    @GetMapping
    fun listAcls(): ResponseEntity<List<AclInfo>> {
        return ResponseEntity.ok(kafkaAclService.listAcls())
    }

    /**
     * ACL 생성
     * POST /admin/kafka/acls
     */
    @PostMapping
    fun createAcl(@RequestBody request: AclCreateRequest): ResponseEntity<Map<String, Any>> {
        val success = kafkaAclService.createAcl(request)
        return if (success) {
            ResponseEntity.ok(
                mapOf(
                    "success" to true,
                    "message" to "ACL 생성 완료",
                    "acl" to mapOf(
                        "resourceType" to request.resourceType,
                        "resourceName" to request.resourceName,
                        "principal" to request.principal,
                        "operation" to request.operation
                    )
                )
            )
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "ACL 생성 실패"))
        }
    }

    /**
     * 여러 ACL 일괄 생성
     * POST /admin/kafka/acls/batch
     */
    @PostMapping("/batch")
    fun createAcls(@RequestBody requests: List<AclCreateRequest>): ResponseEntity<AclBatchResult> {
        val result = kafkaAclService.createAcls(requests)
        return if (result.success) {
            ResponseEntity.ok(result)
        } else {
            ResponseEntity.badRequest().body(result)
        }
    }

    /**
     * 리소스별 ACL 조회
     * GET /admin/kafka/acls/resource/{resourceType}/{resourceName}
     */
    @GetMapping("/resource/{resourceType}/{resourceName}")
    fun getAclsByResource(
        @PathVariable resourceType: AclResourceType,
        @PathVariable resourceName: String
    ): ResponseEntity<List<AclInfo>> {
        return ResponseEntity.ok(kafkaAclService.getAclsByResource(resourceType, resourceName))
    }

    /**
     * Principal별 ACL 조회
     * GET /admin/kafka/acls/principal?principal=User:alice
     */
    @GetMapping("/principal")
    fun getAclsByPrincipal(@RequestParam principal: String): ResponseEntity<List<AclInfo>> {
        return ResponseEntity.ok(kafkaAclService.getAclsByPrincipal(principal))
    }

    /**
     * ACL 삭제
     * DELETE /admin/kafka/acls
     */
    @DeleteMapping
    fun deleteAcl(@RequestBody request: AclDeleteRequest): ResponseEntity<Map<String, Any>> {
        val success = kafkaAclService.deleteAcl(request)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "message" to "ACL 삭제 완료"))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "ACL 삭제 실패 또는 해당 ACL 없음"))
        }
    }

    /**
     * 리소스의 모든 ACL 삭제
     * DELETE /admin/kafka/acls/resource/{resourceType}/{resourceName}
     */
    @DeleteMapping("/resource/{resourceType}/{resourceName}")
    fun deleteAllAclsForResource(
        @PathVariable resourceType: AclResourceType,
        @PathVariable resourceName: String
    ): ResponseEntity<Map<String, Any>> {
        val deletedCount = kafkaAclService.deleteAllAclsForResource(resourceType, resourceName)
        return ResponseEntity.ok(
            mapOf(
                "success" to (deletedCount > 0),
                "deletedCount" to deletedCount,
                "resourceType" to resourceType,
                "resourceName" to resourceName
            )
        )
    }

    /**
     * 프로듀서 권한 부여 (편의 API)
     * POST /admin/kafka/acls/grant/producer
     */
    @PostMapping("/grant/producer")
    fun grantProducerPermission(@RequestBody request: GrantProducerRequest): ResponseEntity<Map<String, Any>> {
        val success = kafkaAclService.grantProducerPermission(request.topicName, request.principal)
        return if (success) {
            ResponseEntity.ok(
                mapOf(
                    "success" to true,
                    "message" to "프로듀서 권한 부여 완료",
                    "topic" to request.topicName,
                    "principal" to request.principal,
                    "permissions" to listOf("WRITE", "DESCRIBE")
                )
            )
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "프로듀서 권한 부여 실패"))
        }
    }

    /**
     * 컨슈머 권한 부여 (편의 API)
     * POST /admin/kafka/acls/grant/consumer
     */
    @PostMapping("/grant/consumer")
    fun grantConsumerPermission(@RequestBody request: GrantConsumerRequest): ResponseEntity<Map<String, Any>> {
        val success = kafkaAclService.grantConsumerPermission(
            request.topicName,
            request.groupId,
            request.principal
        )
        return if (success) {
            ResponseEntity.ok(
                mapOf(
                    "success" to true,
                    "message" to "컨슈머 권한 부여 완료",
                    "topic" to request.topicName,
                    "groupId" to request.groupId,
                    "principal" to request.principal,
                    "permissions" to listOf("READ", "DESCRIBE", "GROUP:READ")
                )
            )
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "컨슈머 권한 부여 실패"))
        }
    }

    /**
     * 관리자 권한 부여 (편의 API)
     * POST /admin/kafka/acls/grant/admin
     */
    @PostMapping("/grant/admin")
    fun grantAdminPermission(@RequestBody request: GrantAdminRequest): ResponseEntity<Map<String, Any>> {
        val success = kafkaAclService.grantAdminPermission(request.topicName, request.principal)
        return if (success) {
            ResponseEntity.ok(
                mapOf(
                    "success" to true,
                    "message" to "관리자 권한 부여 완료",
                    "topic" to request.topicName,
                    "principal" to request.principal,
                    "permissions" to listOf("ALL")
                )
            )
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "관리자 권한 부여 실패"))
        }
    }
}

// 편의 API용 Request DTO
data class GrantProducerRequest(
    val topicName: String,
    val principal: String  // 예: "User:producer-service"
)

data class GrantConsumerRequest(
    val topicName: String,
    val groupId: String,
    val principal: String  // 예: "User:consumer-service"
)

data class GrantAdminRequest(
    val topicName: String,
    val principal: String  // 예: "User:admin"
)