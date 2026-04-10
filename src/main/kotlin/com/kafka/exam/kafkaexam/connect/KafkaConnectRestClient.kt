package com.kafka.exam.kafkaexam.connect

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.ParameterizedTypeReference
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientException

/**
 * Kafka Connect REST API 클라이언트
 *
 * Kafka Connect의 REST API를 1:1로 매핑하는 저수준 클라이언트
 */
@Component
class KafkaConnectRestClient(
    @Qualifier("kafkaConnectRestClient") private val restClient: RestClient
) {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Connect 클러스터 정보 조회
     * GET /
     */
    fun getClusterInfo(): ConnectClusterInfo? {
        return try {
            restClient.get()
                .uri("/")
                .retrieve()
                .body(ConnectClusterInfo::class.java)
        } catch (e: RestClientException) {
            log.error("Connect 클러스터 정보 조회 실패: {}", e.message)
            null
        }
    }

    /**
     * 사용 가능한 커넥터 플러그인 목록 조회
     * GET /connector-plugins
     */
    fun listConnectorPlugins(): List<ConnectorPluginInfo> {
        return try {
            restClient.get()
                .uri("/connector-plugins")
                .retrieve()
                .body(object : ParameterizedTypeReference<List<ConnectorPluginInfo>>() {}) ?: emptyList()
        } catch (e: RestClientException) {
            log.error("커넥터 플러그인 목록 조회 실패: {}", e.message)
            emptyList()
        }
    }

    /**
     * 커넥터 설정 유효성 검증
     * PUT /connector-plugins/{pluginName}/config/validate
     */
    fun validateConnectorConfig(pluginName: String, config: Map<String, String>): ConfigValidationResult? {
        return try {
            restClient.put()
                .uri("/connector-plugins/{pluginName}/config/validate", pluginName)
                .body(config)
                .retrieve()
                .body(ConfigValidationResult::class.java)
        } catch (e: RestClientException) {
            log.error("커넥터 설정 검증 실패: plugin={}, error={}", pluginName, e.message)
            null
        }
    }

    /**
     * 모든 커넥터 이름 목록 조회
     * GET /connectors
     */
    fun listConnectors(): List<String> {
        return try {
            restClient.get()
                .uri("/connectors")
                .retrieve()
                .body(object : ParameterizedTypeReference<List<String>>() {}) ?: emptyList()
        } catch (e: RestClientException) {
            log.error("커넥터 목록 조회 실패: {}", e.message)
            emptyList()
        }
    }

    /**
     * 커넥터 정보 조회
     * GET /connectors/{name}
     */
    fun getConnector(name: String): ConnectorInfo? {
        return try {
            restClient.get()
                .uri("/connectors/{name}", name)
                .retrieve()
                .body(ConnectorInfo::class.java)
        } catch (e: RestClientException) {
            log.error("커넥터 조회 실패: name={}, error={}", name, e.message)
            null
        }
    }

    /**
     * 커넥터 생성
     * POST /connectors
     */
    fun createConnector(request: CreateConnectorRequest): ConnectorInfo? {
        return try {
            restClient.post()
                .uri("/connectors")
                .body(request)
                .retrieve()
                .body(ConnectorInfo::class.java)
        } catch (e: RestClientException) {
            log.error("커넥터 생성 실패: name={}, error={}", request.name, e.message)
            null
        }
    }

    /**
     * 커넥터 설정 업데이트
     * PUT /connectors/{name}/config
     */
    fun updateConnectorConfig(name: String, config: Map<String, String>): ConnectorInfo? {
        return try {
            restClient.put()
                .uri("/connectors/{name}/config", name)
                .body(config)
                .retrieve()
                .body(ConnectorInfo::class.java)
        } catch (e: RestClientException) {
            log.error("커넥터 설정 업데이트 실패: name={}, error={}", name, e.message)
            null
        }
    }

    /**
     * 커넥터 삭제
     * DELETE /connectors/{name}
     */
    fun deleteConnector(name: String): Boolean {
        return try {
            restClient.delete()
                .uri("/connectors/{name}", name)
                .retrieve()
                .toBodilessEntity()
            log.info("커넥터 삭제 완료: {}", name)
            true
        } catch (e: RestClientException) {
            log.error("커넥터 삭제 실패: name={}, error={}", name, e.message)
            false
        }
    }

    /**
     * 커넥터 상태 조회
     * GET /connectors/{name}/status
     */
    fun getConnectorStatus(name: String): ConnectorStatus? {
        return try {
            restClient.get()
                .uri("/connectors/{name}/status", name)
                .retrieve()
                .body(ConnectorStatus::class.java)
        } catch (e: RestClientException) {
            log.error("커넥터 상태 조회 실패: name={}, error={}", name, e.message)
            null
        }
    }

    /**
     * 커넥터 설정 조회
     * GET /connectors/{name}/config
     */
    fun getConnectorConfig(name: String): Map<String, String> {
        return try {
            restClient.get()
                .uri("/connectors/{name}/config", name)
                .retrieve()
                .body(object : ParameterizedTypeReference<Map<String, String>>() {}) ?: emptyMap()
        } catch (e: RestClientException) {
            log.error("커넥터 설정 조회 실패: name={}, error={}", name, e.message)
            emptyMap()
        }
    }

    /**
     * 커넥터 태스크 목록 조회
     * GET /connectors/{name}/tasks
     */
    fun getConnectorTasks(name: String): List<TaskInfo> {
        return try {
            restClient.get()
                .uri("/connectors/{name}/tasks", name)
                .retrieve()
                .body(object : ParameterizedTypeReference<List<TaskInfo>>() {}) ?: emptyList()
        } catch (e: RestClientException) {
            log.error("커넥터 태스크 조회 실패: name={}, error={}", name, e.message)
            emptyList()
        }
    }

    /**
     * 태스크 상태 조회
     * GET /connectors/{name}/tasks/{taskId}/status
     */
    fun getTaskStatus(name: String, taskId: Int): TaskStatusInfo? {
        return try {
            restClient.get()
                .uri("/connectors/{name}/tasks/{taskId}/status", name, taskId)
                .retrieve()
                .body(TaskStatusInfo::class.java)
        } catch (e: RestClientException) {
            log.error("태스크 상태 조회 실패: name={}, taskId={}, error={}", name, taskId, e.message)
            null
        }
    }

    /**
     * 커넥터 일시정지
     * PUT /connectors/{name}/pause
     */
    fun pauseConnector(name: String): Boolean {
        return try {
            restClient.put()
                .uri("/connectors/{name}/pause", name)
                .retrieve()
                .toBodilessEntity()
            log.info("커넥터 일시정지: {}", name)
            true
        } catch (e: RestClientException) {
            log.error("커넥터 일시정지 실패: name={}, error={}", name, e.message)
            false
        }
    }

    /**
     * 커넥터 재개
     * PUT /connectors/{name}/resume
     */
    fun resumeConnector(name: String): Boolean {
        return try {
            restClient.put()
                .uri("/connectors/{name}/resume", name)
                .retrieve()
                .toBodilessEntity()
            log.info("커넥터 재개: {}", name)
            true
        } catch (e: RestClientException) {
            log.error("커넥터 재개 실패: name={}, error={}", name, e.message)
            false
        }
    }

    /**
     * 커넥터 재시작
     * POST /connectors/{name}/restart
     */
    fun restartConnector(name: String): Boolean {
        return try {
            restClient.post()
                .uri("/connectors/{name}/restart", name)
                .retrieve()
                .toBodilessEntity()
            log.info("커넥터 재시작: {}", name)
            true
        } catch (e: RestClientException) {
            log.error("커넥터 재시작 실패: name={}, error={}", name, e.message)
            false
        }
    }

    /**
     * 태스크 재시작
     * POST /connectors/{name}/tasks/{taskId}/restart
     */
    fun restartTask(name: String, taskId: Int): Boolean {
        return try {
            restClient.post()
                .uri("/connectors/{name}/tasks/{taskId}/restart", name, taskId)
                .retrieve()
                .toBodilessEntity()
            log.info("태스크 재시작: connector={}, taskId={}", name, taskId)
            true
        } catch (e: RestClientException) {
            log.error("태스크 재시작 실패: name={}, taskId={}, error={}", name, taskId, e.message)
            false
        }
    }
}

// === DTOs ===

data class ConnectClusterInfo(
    val version: String = "",
    val commit: String = "",
    val kafka_cluster_id: String = ""
)

data class ConnectorPluginInfo(
    val `class`: String = "",
    val type: String = "",
    val version: String = ""
)

data class ConfigValidationResult(
    val name: String = "",
    val error_count: Int = 0,
    val groups: List<String> = emptyList(),
    val configs: List<ConfigValidationValue> = emptyList()
)

data class ConfigValidationValue(
    val definition: ConfigDefinition? = null,
    val value: ConfigValue? = null
)

data class ConfigDefinition(
    val name: String = "",
    val type: String = "",
    val required: Boolean = false,
    val default_value: String? = null,
    val importance: String = "",
    val documentation: String = ""
)

data class ConfigValue(
    val name: String = "",
    val value: String? = null,
    val recommended_values: List<String> = emptyList(),
    val errors: List<String> = emptyList(),
    val visible: Boolean = true
)

data class CreateConnectorRequest(
    val name: String,
    val config: Map<String, String>
)

data class ConnectorInfo(
    val name: String = "",
    val config: Map<String, String> = emptyMap(),
    val tasks: List<ConnectorTaskId> = emptyList(),
    val type: String = ""
)

data class ConnectorTaskId(
    val connector: String = "",
    val task: Int = 0
)

data class ConnectorStatus(
    val name: String = "",
    val connector: ConnectorStateInfo = ConnectorStateInfo(),
    val tasks: List<TaskStatusInfo> = emptyList(),
    val type: String = ""
)

data class ConnectorStateInfo(
    val state: String = "",
    val worker_id: String = ""
)

data class TaskStatusInfo(
    val id: Int = 0,
    val state: String = "",
    val worker_id: String = "",
    val trace: String? = null
)

data class TaskInfo(
    val id: ConnectorTaskId = ConnectorTaskId(),
    val config: Map<String, String> = emptyMap()
)