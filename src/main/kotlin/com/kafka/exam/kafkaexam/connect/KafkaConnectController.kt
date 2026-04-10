package com.kafka.exam.kafkaexam.connect

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

/**
 * Kafka Connect 관리 REST API
 *
 * 커넥터 CRUD, 라이프사이클, 템플릿 기반 생성 제공
 */
@RestController
@RequestMapping("/admin/kafka/connect")
class KafkaConnectController(
    private val kafkaConnectService: KafkaConnectService
) {

    // === 클러스터 정보 ===

    /**
     * Connect 클러스터 정보 조회
     * GET /admin/kafka/connect/cluster
     */
    @GetMapping("/cluster")
    fun getClusterInfo(): ResponseEntity<ConnectClusterInfo> {
        val info = kafkaConnectService.getClusterInfo()
        return if (info != null) {
            ResponseEntity.ok(info)
        } else {
            ResponseEntity.status(503).build()
        }
    }

    /**
     * 사용 가능한 커넥터 플러그인 목록 조회
     * GET /admin/kafka/connect/plugins
     */
    @GetMapping("/plugins")
    fun listPlugins(): ResponseEntity<List<ConnectorPluginInfo>> {
        return ResponseEntity.ok(kafkaConnectService.listPlugins())
    }

    /**
     * 커넥터 설정 유효성 검증
     * PUT /admin/kafka/connect/plugins/{pluginName}/validate
     */
    @PutMapping("/plugins/{pluginName}/validate")
    fun validateConfig(
        @PathVariable pluginName: String,
        @RequestBody config: Map<String, String>
    ): ResponseEntity<ConfigValidationResult> {
        val result = kafkaConnectService.validateConfig(pluginName, config)
        return if (result != null) {
            ResponseEntity.ok(result)
        } else {
            ResponseEntity.badRequest().build()
        }
    }

    // === 커넥터 CRUD ===

    /**
     * 모든 커넥터 목록 (상태 포함) 조회
     * GET /admin/kafka/connect/connectors
     */
    @GetMapping("/connectors")
    fun listConnectors(): ResponseEntity<List<ConnectorSummary>> {
        return ResponseEntity.ok(kafkaConnectService.listConnectorsWithStatus())
    }

    /**
     * 커넥터 생성
     * POST /admin/kafka/connect/connectors
     */
    @PostMapping("/connectors")
    fun createConnector(@RequestBody request: CreateConnectorRequest): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.createConnector(request)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "커넥터 생성 실패"))
        }
    }

    /**
     * 커넥터 상세 정보 조회
     * GET /admin/kafka/connect/connectors/{name}
     */
    @GetMapping("/connectors/{name}")
    fun getConnector(@PathVariable name: String): ResponseEntity<ConnectorDetail> {
        val detail = kafkaConnectService.getConnectorDetail(name)
        return if (detail != null) {
            ResponseEntity.ok(detail)
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * 커넥터 설정 업데이트
     * PUT /admin/kafka/connect/connectors/{name}/config
     */
    @PutMapping("/connectors/{name}/config")
    fun updateConnectorConfig(
        @PathVariable name: String,
        @RequestBody config: Map<String, String>
    ): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.updateConnector(name, config)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "커넥터 설정 업데이트 실패"))
        }
    }

    /**
     * 커넥터 삭제
     * DELETE /admin/kafka/connect/connectors/{name}
     */
    @DeleteMapping("/connectors/{name}")
    fun deleteConnector(@PathVariable name: String): ResponseEntity<Map<String, Any>> {
        val success = kafkaConnectService.deleteConnector(name)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to name))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "커넥터 삭제 실패"))
        }
    }

    // === 라이프사이클 관리 ===

    /**
     * 커넥터 상태 조회
     * GET /admin/kafka/connect/connectors/{name}/status
     */
    @GetMapping("/connectors/{name}/status")
    fun getConnectorStatus(@PathVariable name: String): ResponseEntity<ConnectorStatus> {
        val status = kafkaConnectService.getConnectorDetail(name)
        return if (status != null) {
            ResponseEntity.ok(
                ConnectorStatus(
                    name = status.name,
                    connector = ConnectorStateInfo(state = status.state, worker_id = status.workerId),
                    tasks = status.tasks,
                    type = status.type
                )
            )
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * 커넥터 일시정지
     * PUT /admin/kafka/connect/connectors/{name}/pause
     */
    @PutMapping("/connectors/{name}/pause")
    fun pauseConnector(@PathVariable name: String): ResponseEntity<Map<String, Any>> {
        val success = kafkaConnectService.pauseConnector(name)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to name, "action" to "paused"))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "커넥터 일시정지 실패"))
        }
    }

    /**
     * 커넥터 재개
     * PUT /admin/kafka/connect/connectors/{name}/resume
     */
    @PutMapping("/connectors/{name}/resume")
    fun resumeConnector(@PathVariable name: String): ResponseEntity<Map<String, Any>> {
        val success = kafkaConnectService.resumeConnector(name)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to name, "action" to "resumed"))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "커넥터 재개 실패"))
        }
    }

    /**
     * 커넥터 재시작
     * POST /admin/kafka/connect/connectors/{name}/restart
     */
    @PostMapping("/connectors/{name}/restart")
    fun restartConnector(
        @PathVariable name: String,
        @RequestParam(defaultValue = "false") includeTasks: Boolean
    ): ResponseEntity<Map<String, Any>> {
        val success = kafkaConnectService.restartConnector(name, includeTasks)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to name, "action" to "restarted", "includeTasks" to includeTasks))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "커넥터 재시작 실패"))
        }
    }

    /**
     * 특정 태스크 재시작
     * POST /admin/kafka/connect/connectors/{name}/tasks/{taskId}/restart
     */
    @PostMapping("/connectors/{name}/tasks/{taskId}/restart")
    fun restartTask(
        @PathVariable name: String,
        @PathVariable taskId: Int
    ): ResponseEntity<Map<String, Any>> {
        val success = kafkaConnectService.restartConnector(name)
        return if (success) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to name, "taskId" to taskId, "action" to "task_restarted"))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "태스크 재시작 실패"))
        }
    }

    /**
     * 실패한 태스크만 재시작
     * POST /admin/kafka/connect/connectors/{name}/restart-failed-tasks
     */
    @PostMapping("/connectors/{name}/restart-failed-tasks")
    fun restartFailedTasks(@PathVariable name: String): ResponseEntity<Map<String, Any>> {
        val count = kafkaConnectService.restartFailedTasks(name)
        return ResponseEntity.ok(mapOf("success" to true, "connector" to name, "restartedTasks" to count))
    }

    /**
     * 모든 실패한 커넥터 일괄 재시작
     * POST /admin/kafka/connect/connectors/restart-all-failed
     */
    @PostMapping("/connectors/restart-all-failed")
    fun restartAllFailed(): ResponseEntity<Map<String, Any>> {
        val results = kafkaConnectService.restartAllFailedConnectors()
        return ResponseEntity.ok(mapOf("success" to true, "results" to results))
    }

    // === 사전 정의 템플릿 ===

    /**
     * JDBC Source Connector 템플릿으로 생성
     * POST /admin/kafka/connect/connectors/templates/jdbc-source
     */
    @PostMapping("/connectors/templates/jdbc-source")
    fun createJdbcSource(@RequestBody request: JdbcSourceRequest): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.createJdbcSourceConnector(request)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "JDBC Source Connector 생성 실패"))
        }
    }

    /**
     * JDBC Sink Connector 템플릿으로 생성
     * POST /admin/kafka/connect/connectors/templates/jdbc-sink
     */
    @PostMapping("/connectors/templates/jdbc-sink")
    fun createJdbcSink(@RequestBody request: JdbcSinkRequest): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.createJdbcSinkConnector(request)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "JDBC Sink Connector 생성 실패"))
        }
    }

    /**
     * File Source Connector 템플릿으로 생성
     * POST /admin/kafka/connect/connectors/templates/file-source
     */
    @PostMapping("/connectors/templates/file-source")
    fun createFileSource(@RequestBody request: FileSourceRequest): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.createFileSourceConnector(request)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "File Source Connector 생성 실패"))
        }
    }

    /**
     * File Sink Connector 템플릿으로 생성
     * POST /admin/kafka/connect/connectors/templates/file-sink
     */
    @PostMapping("/connectors/templates/file-sink")
    fun createFileSink(@RequestBody request: FileSinkRequest): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.createFileSinkConnector(request)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "File Sink Connector 생성 실패"))
        }
    }

    /**
     * Elasticsearch Sink Connector 템플릿으로 생성
     * POST /admin/kafka/connect/connectors/templates/elasticsearch-sink
     */
    @PostMapping("/connectors/templates/elasticsearch-sink")
    fun createElasticsearchSink(@RequestBody request: ElasticsearchSinkRequest): ResponseEntity<Map<String, Any>> {
        val result = kafkaConnectService.createElasticsearchSinkConnector(request)
        return if (result != null) {
            ResponseEntity.ok(mapOf("success" to true, "connector" to result))
        } else {
            ResponseEntity.badRequest().body(mapOf("success" to false, "error" to "Elasticsearch Sink Connector 생성 실패"))
        }
    }
}