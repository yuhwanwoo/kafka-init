package com.kafka.exam.kafkaexam.connect

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Kafka Connect 관리 서비스
 *
 * 커넥터 CRUD, 라이프사이클 관리, 사전 정의 템플릿 제공
 */
@Service
class KafkaConnectService(
    private val connectClient: KafkaConnectRestClient
) {

    private val log = LoggerFactory.getLogger(javaClass)

    // === 커넥터 조회 ===

    /**
     * 모든 커넥터 요약 정보 조회 (이름 + 상태)
     */
    fun listConnectorsWithStatus(): List<ConnectorSummary> {
        val names = connectClient.listConnectors()
        return names.mapNotNull { name ->
            val status = connectClient.getConnectorStatus(name)
            status?.let {
                ConnectorSummary(
                    name = it.name,
                    type = it.type,
                    state = it.connector.state,
                    workerId = it.connector.worker_id,
                    taskCount = it.tasks.size,
                    runningTasks = it.tasks.count { task -> task.state == "RUNNING" },
                    failedTasks = it.tasks.count { task -> task.state == "FAILED" }
                )
            }
        }
    }

    /**
     * 커넥터 상세 정보 조회 (설정 + 상태 + 태스크)
     */
    fun getConnectorDetail(name: String): ConnectorDetail? {
        val info = connectClient.getConnector(name) ?: return null
        val status = connectClient.getConnectorStatus(name)
        val tasks = connectClient.getConnectorTasks(name)

        return ConnectorDetail(
            name = info.name,
            type = info.type,
            config = info.config,
            state = status?.connector?.state ?: "UNKNOWN",
            workerId = status?.connector?.worker_id ?: "",
            tasks = status?.tasks ?: emptyList()
        )
    }

    // === 커넥터 CRUD ===

    /**
     * 커넥터 생성
     */
    fun createConnector(request: CreateConnectorRequest): ConnectorInfo? {
        log.info("커넥터 생성 요청: {}", request.name)
        return connectClient.createConnector(request)
    }

    /**
     * 커넥터 설정 업데이트
     */
    fun updateConnector(name: String, config: Map<String, String>): ConnectorInfo? {
        log.info("커넥터 설정 업데이트: {}", name)
        return connectClient.updateConnectorConfig(name, config)
    }

    /**
     * 커넥터 삭제
     */
    fun deleteConnector(name: String): Boolean {
        log.info("커넥터 삭제 요청: {}", name)
        return connectClient.deleteConnector(name)
    }

    // === 라이프사이클 관리 ===

    /**
     * 커넥터 일시정지
     */
    fun pauseConnector(name: String): Boolean = connectClient.pauseConnector(name)

    /**
     * 커넥터 재개
     */
    fun resumeConnector(name: String): Boolean = connectClient.resumeConnector(name)

    /**
     * 커넥터 재시작 (실패한 태스크도 함께 재시작 옵션)
     */
    fun restartConnector(name: String, includeTasks: Boolean = false): Boolean {
        val result = connectClient.restartConnector(name)
        if (result && includeTasks) {
            restartFailedTasks(name)
        }
        return result
    }

    /**
     * 실패한 태스크만 재시작
     */
    fun restartFailedTasks(name: String): Int {
        val status = connectClient.getConnectorStatus(name) ?: return 0
        var restartedCount = 0

        status.tasks
            .filter { it.state == "FAILED" }
            .forEach { task ->
                if (connectClient.restartTask(name, task.id)) {
                    restartedCount++
                }
            }

        log.info("커넥터 '{}' 실패 태스크 {} 개 재시작", name, restartedCount)
        return restartedCount
    }

    /**
     * 모든 실패한 커넥터 재시작
     */
    fun restartAllFailedConnectors(): Map<String, Boolean> {
        val results = mutableMapOf<String, Boolean>()
        val connectors = listConnectorsWithStatus()

        connectors
            .filter { it.state == "FAILED" }
            .forEach { connector ->
                results[connector.name] = restartConnector(connector.name, includeTasks = true)
            }

        log.info("실패한 커넥터 일괄 재시작 결과: {}", results)
        return results
    }

    // === 설정 검증 ===

    /**
     * 커넥터 설정 유효성 검증
     */
    fun validateConfig(pluginName: String, config: Map<String, String>): ConfigValidationResult? {
        return connectClient.validateConnectorConfig(pluginName, config)
    }

    // === 사전 정의 커넥터 템플릿 ===

    /**
     * JDBC Source Connector 생성
     */
    fun createJdbcSourceConnector(request: JdbcSourceRequest): ConnectorInfo? {
        val config = mutableMapOf(
            "connector.class" to "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url" to request.connectionUrl,
            "connection.user" to request.connectionUser,
            "connection.password" to request.connectionPassword,
            "table.whitelist" to request.tableWhitelist,
            "mode" to request.mode,
            "topic.prefix" to request.topicPrefix,
            "poll.interval.ms" to request.pollIntervalMs.toString(),
            "tasks.max" to request.tasksMax.toString()
        )

        if (request.mode == "incrementing" || request.mode == "timestamp+incrementing") {
            config["incrementing.column.name"] = request.incrementingColumnName
        }
        if (request.mode == "timestamp" || request.mode == "timestamp+incrementing") {
            request.timestampColumnName?.let { config["timestamp.column.name"] = it }
        }

        log.info("JDBC Source Connector 생성: {}", request.name)
        return connectClient.createConnector(CreateConnectorRequest(request.name, config))
    }

    /**
     * JDBC Sink Connector 생성
     */
    fun createJdbcSinkConnector(request: JdbcSinkRequest): ConnectorInfo? {
        val config = mapOf(
            "connector.class" to "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url" to request.connectionUrl,
            "connection.user" to request.connectionUser,
            "connection.password" to request.connectionPassword,
            "topics" to request.topics,
            "insert.mode" to request.insertMode,
            "auto.create" to request.autoCreate.toString(),
            "auto.evolve" to request.autoEvolve.toString(),
            "pk.mode" to request.pkMode,
            "pk.fields" to request.pkFields,
            "tasks.max" to request.tasksMax.toString()
        )

        log.info("JDBC Sink Connector 생성: {}", request.name)
        return connectClient.createConnector(CreateConnectorRequest(request.name, config))
    }

    /**
     * File Source Connector 생성
     */
    fun createFileSourceConnector(request: FileSourceRequest): ConnectorInfo? {
        val config = mapOf(
            "connector.class" to "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "file" to request.filePath,
            "topic" to request.topic,
            "tasks.max" to request.tasksMax.toString()
        )

        log.info("File Source Connector 생성: {}", request.name)
        return connectClient.createConnector(CreateConnectorRequest(request.name, config))
    }

    /**
     * File Sink Connector 생성
     */
    fun createFileSinkConnector(request: FileSinkRequest): ConnectorInfo? {
        val config = mapOf(
            "connector.class" to "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "file" to request.filePath,
            "topics" to request.topics,
            "tasks.max" to request.tasksMax.toString()
        )

        log.info("File Sink Connector 생성: {}", request.name)
        return connectClient.createConnector(CreateConnectorRequest(request.name, config))
    }

    /**
     * Elasticsearch Sink Connector 생성
     */
    fun createElasticsearchSinkConnector(request: ElasticsearchSinkRequest): ConnectorInfo? {
        val config = mutableMapOf(
            "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
            "connection.url" to request.connectionUrl,
            "topics" to request.topics,
            "type.name" to request.typeName,
            "key.ignore" to request.keyIgnore.toString(),
            "schema.ignore" to request.schemaIgnore.toString(),
            "tasks.max" to request.tasksMax.toString()
        )

        request.transforms?.let { config["transforms"] = it }

        log.info("Elasticsearch Sink Connector 생성: {}", request.name)
        return connectClient.createConnector(CreateConnectorRequest(request.name, config))
    }

    // === 클러스터 정보 ===

    /**
     * Connect 클러스터 정보 조회
     */
    fun getClusterInfo(): ConnectClusterInfo? = connectClient.getClusterInfo()

    /**
     * 사용 가능한 커넥터 플러그인 목록
     */
    fun listPlugins(): List<ConnectorPluginInfo> = connectClient.listConnectorPlugins()
}

// === DTOs ===

data class ConnectorSummary(
    val name: String,
    val type: String,
    val state: String,
    val workerId: String,
    val taskCount: Int,
    val runningTasks: Int,
    val failedTasks: Int
)

data class ConnectorDetail(
    val name: String,
    val type: String,
    val config: Map<String, String>,
    val state: String,
    val workerId: String,
    val tasks: List<TaskStatusInfo>
)

data class JdbcSourceRequest(
    val name: String,
    val connectionUrl: String,
    val connectionUser: String,
    val connectionPassword: String,
    val tableWhitelist: String,
    val mode: String = "incrementing",
    val incrementingColumnName: String = "id",
    val timestampColumnName: String? = null,
    val topicPrefix: String = "jdbc-",
    val pollIntervalMs: Long = 5000,
    val tasksMax: Int = 1
)

data class JdbcSinkRequest(
    val name: String,
    val connectionUrl: String,
    val connectionUser: String,
    val connectionPassword: String,
    val topics: String,
    val insertMode: String = "upsert",
    val autoCreate: Boolean = true,
    val autoEvolve: Boolean = true,
    val pkMode: String = "record_key",
    val pkFields: String = "id",
    val tasksMax: Int = 1
)

data class FileSourceRequest(
    val name: String,
    val filePath: String,
    val topic: String,
    val tasksMax: Int = 1
)

data class FileSinkRequest(
    val name: String,
    val filePath: String,
    val topics: String,
    val tasksMax: Int = 1
)

data class ElasticsearchSinkRequest(
    val name: String,
    val connectionUrl: String,
    val topics: String,
    val typeName: String = "_doc",
    val keyIgnore: Boolean = true,
    val schemaIgnore: Boolean = true,
    val transforms: String? = null,
    val tasksMax: Int = 1
)