package com.kafka.exam.kafkaexam.connect

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.slf4j.LoggerFactory
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import jakarta.annotation.PostConstruct

/**
 * Kafka Connect Health Indicator
 *
 * /actuator/health에서 Kafka Connect 상태 확인 가능
 */
@Component("kafkaConnectHealth")
class KafkaConnectHealthIndicator(
    private val connectClient: KafkaConnectRestClient
) : HealthIndicator {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun health(): Health {
        return try {
            val clusterInfo = connectClient.getClusterInfo()
                ?: return Health.down()
                    .withDetail("error", "Kafka Connect 클러스터에 연결할 수 없습니다")
                    .build()

            val connectors = connectClient.listConnectors()
            var runningCount = 0
            var failedCount = 0
            var pausedCount = 0
            val failedConnectors = mutableListOf<String>()

            connectors.forEach { name ->
                val status = connectClient.getConnectorStatus(name)
                when (status?.connector?.state) {
                    "RUNNING" -> runningCount++
                    "FAILED" -> {
                        failedCount++
                        failedConnectors.add(name)
                    }
                    "PAUSED" -> pausedCount++
                }
            }

            val builder = when {
                failedCount > 0 -> Health.down()
                pausedCount > 0 -> Health.status("DEGRADED")
                else -> Health.up()
            }

            builder
                .withDetail("connectVersion", clusterInfo.version)
                .withDetail("kafkaClusterId", clusterInfo.kafka_cluster_id)
                .withDetail("connectorCount", connectors.size)
                .withDetail("runningCount", runningCount)
                .withDetail("failedCount", failedCount)
                .withDetail("pausedCount", pausedCount)
                .withDetail("failedConnectors", failedConnectors)
                .build()

        } catch (e: Exception) {
            log.error("Kafka Connect health check failed: {}", e.message)
            Health.down()
                .withException(e)
                .withDetail("error", e.message ?: "Unknown error")
                .build()
        }
    }
}

/**
 * Kafka Connect 메트릭 수집기
 *
 * Micrometer를 사용하여 Connect 클러스터 메트릭 수집
 */
@Component
class KafkaConnectMetricsCollector(
    private val meterRegistry: MeterRegistry,
    private val connectClient: KafkaConnectRestClient
) {

    private val log = LoggerFactory.getLogger(javaClass)

    private val totalConnectors = AtomicLong(0)
    private val runningConnectors = AtomicLong(0)
    private val failedConnectors = AtomicLong(0)
    private val pausedConnectors = AtomicLong(0)
    private val totalTasks = AtomicLong(0)
    private val runningTasks = AtomicLong(0)
    private val failedTasks = AtomicLong(0)
    private val clusterReachable = AtomicLong(0)

    private val connectorStates = ConcurrentHashMap<String, String>()

    @PostConstruct
    fun init() {
        registerMetrics()
        log.info("KafkaConnectMetricsCollector initialized")
    }

    private fun registerMetrics() {
        Gauge.builder("kafka.connect.connectors.total") { totalConnectors.get().toDouble() }
            .description("Total number of connectors")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.connectors.running") { runningConnectors.get().toDouble() }
            .description("Number of running connectors")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.connectors.failed") { failedConnectors.get().toDouble() }
            .description("Number of failed connectors")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.connectors.paused") { pausedConnectors.get().toDouble() }
            .description("Number of paused connectors")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.tasks.total") { totalTasks.get().toDouble() }
            .description("Total number of tasks")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.tasks.running") { runningTasks.get().toDouble() }
            .description("Number of running tasks")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.tasks.failed") { failedTasks.get().toDouble() }
            .description("Number of failed tasks")
            .register(meterRegistry)

        Gauge.builder("kafka.connect.cluster.reachable") { clusterReachable.get().toDouble() }
            .description("Kafka Connect cluster reachability (1=reachable, 0=unreachable)")
            .register(meterRegistry)
    }

    @Scheduled(fixedDelayString = "\${kafka.connect.health-check-interval-ms:30000}")
    fun collectConnectMetrics() {
        try {
            val clusterInfo = connectClient.getClusterInfo()
            if (clusterInfo == null) {
                clusterReachable.set(0)
                return
            }
            clusterReachable.set(1)

            val connectors = connectClient.listConnectors()
            totalConnectors.set(connectors.size.toLong())

            var running = 0L
            var failed = 0L
            var paused = 0L
            var taskTotal = 0L
            var taskRunning = 0L
            var taskFailed = 0L

            connectors.forEach { name ->
                val status = connectClient.getConnectorStatus(name)
                if (status != null) {
                    val state = status.connector.state
                    when (state) {
                        "RUNNING" -> running++
                        "FAILED" -> failed++
                        "PAUSED" -> paused++
                    }

                    // 개별 커넥터 상태 변경 감지 및 메트릭 등록
                    val prevState = connectorStates.put(name, state)
                    if (prevState != state) {
                        registerConnectorStateMetric(name, state)
                        if (prevState != null) {
                            log.info("커넥터 '{}' 상태 변경: {} -> {}", name, prevState, state)
                        }
                    }

                    // 태스크 메트릭
                    taskTotal += status.tasks.size
                    status.tasks.forEach { task ->
                        when (task.state) {
                            "RUNNING" -> taskRunning++
                            "FAILED" -> taskFailed++
                        }
                    }
                }
            }

            runningConnectors.set(running)
            failedConnectors.set(failed)
            pausedConnectors.set(paused)
            totalTasks.set(taskTotal)
            runningTasks.set(taskRunning)
            failedTasks.set(taskFailed)

            // 삭제된 커넥터 정리
            connectorStates.keys.removeAll { it !in connectors }

        } catch (e: Exception) {
            log.error("Kafka Connect 메트릭 수집 실패: {}", e.message)
            clusterReachable.set(0)
        }
    }

    private fun registerConnectorStateMetric(name: String, state: String) {
        Gauge.builder("kafka.connect.connector.state") {
            if (connectorStates[name] == "RUNNING") 1.0 else 0.0
        }
            .description("Individual connector state (1=RUNNING, 0=other)")
            .tags(Tags.of("connector", name, "state", state))
            .register(meterRegistry)
    }

    fun getMetricsSummary(): ConnectMetricsSummary {
        return ConnectMetricsSummary(
            clusterReachable = clusterReachable.get() == 1L,
            totalConnectors = totalConnectors.get(),
            runningConnectors = runningConnectors.get(),
            failedConnectors = failedConnectors.get(),
            pausedConnectors = pausedConnectors.get(),
            totalTasks = totalTasks.get(),
            runningTasks = runningTasks.get(),
            failedTasks = failedTasks.get(),
            connectorStates = connectorStates.toMap()
        )
    }
}

data class ConnectMetricsSummary(
    val clusterReachable: Boolean,
    val totalConnectors: Long,
    val runningConnectors: Long,
    val failedConnectors: Long,
    val pausedConnectors: Long,
    val totalTasks: Long,
    val runningTasks: Long,
    val failedTasks: Long,
    val connectorStates: Map<String, String>
)