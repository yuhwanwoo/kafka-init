package com.kafka.exam.kafkaexam.metrics

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy

/**
 * Kafka 클러스터 Health Indicator
 *
 * /actuator/health에서 Kafka 상태 확인 가능
 */
@Component("kafkaClusterHealth")
class KafkaHealthIndicator(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${kafka.health.timeout-ms:5000}") private val timeoutMs: Long,
    private val consumerLagMonitor: ConsumerLagMonitor
) : HealthIndicator {

    private val log = LoggerFactory.getLogger(javaClass)
    private lateinit var adminClient: AdminClient

    @PostConstruct
    fun init() {
        adminClient = AdminClient.create(mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to timeoutMs.toInt()
        ))
    }

    @PreDestroy
    fun cleanup() {
        adminClient.close(Duration.ofSeconds(5))
    }

    override fun health(): Health {
        return try {
            val healthBuilder = Health.up()

            // 1. 클러스터 연결 확인
            val clusterInfo = checkClusterConnection()
            healthBuilder.withDetails(clusterInfo)

            // 2. 브로커 상태 확인
            val brokerInfo = checkBrokers()
            healthBuilder.withDetails(brokerInfo)

            // 3. Consumer Lag 확인
            val lagInfo = checkConsumerLag()
            healthBuilder.withDetails(lagInfo)

            // 4. 알림 상태 확인
            val alertInfo = checkAlerts()
            if (alertInfo["hasAlerts"] == true) {
                healthBuilder.status("DEGRADED")
            }
            healthBuilder.withDetails(alertInfo)

            healthBuilder.build()

        } catch (e: Exception) {
            log.error("Kafka health check failed: {}", e.message)
            Health.down()
                .withException(e)
                .withDetail("error", e.message ?: "Unknown error")
                .build()
        }
    }

    private fun checkClusterConnection(): Map<String, Any> {
        val options = DescribeClusterOptions().timeoutMs(timeoutMs.toInt())
        val result = adminClient.describeCluster(options)

        val clusterId = result.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS)
        val controller = result.controller().get(timeoutMs, TimeUnit.MILLISECONDS)

        return mapOf(
            "clusterId" to clusterId,
            "controllerId" to controller.id(),
            "controllerHost" to "${controller.host()}:${controller.port()}"
        )
    }

    private fun checkBrokers(): Map<String, Any> {
        val options = DescribeClusterOptions().timeoutMs(timeoutMs.toInt())
        val result = adminClient.describeCluster(options)

        val nodes = result.nodes().get(timeoutMs, TimeUnit.MILLISECONDS)

        return mapOf(
            "brokerCount" to nodes.size,
            "brokers" to nodes.map { "${it.host()}:${it.port()} (id=${it.id()})" }
        )
    }

    private fun checkConsumerLag(): Map<String, Any> {
        val alerts = consumerLagMonitor.getActiveAlerts()
        val criticalCount = alerts.count { it.value == AlertLevel.CRITICAL }
        val warningCount = alerts.count { it.value == AlertLevel.WARNING }

        return mapOf(
            "consumerLag" to mapOf(
                "criticalAlerts" to criticalCount,
                "warningAlerts" to warningCount,
                "totalAlerts" to alerts.size
            )
        )
    }

    private fun checkAlerts(): Map<String, Any> {
        val alerts = consumerLagMonitor.getActiveAlerts(AlertLevel.WARNING)

        return mapOf(
            "hasAlerts" to alerts.isNotEmpty(),
            "activeAlerts" to alerts.size,
            "alertDetails" to alerts.map { "${it.key}: ${it.value}" }
        )
    }
}

/**
 * Kafka Producer Health Indicator
 */
@Component("kafkaProducerHealth")
class KafkaProducerHealthIndicator(
    private val kafkaMetricsCollector: KafkaMetricsCollector
) : HealthIndicator {

    override fun health(): Health {
        val summary = kafkaMetricsCollector.getMetricsSummary()

        val successRate = if (summary.messagesProduced > 0) {
            ((summary.messagesProduced - summary.producerErrors).toDouble() / summary.messagesProduced) * 100
        } else {
            100.0
        }

        val builder = when {
            successRate < 90 -> Health.down()
            successRate < 99 -> Health.status("DEGRADED")
            else -> Health.up()
        }

        return builder
            .withDetail("messagesProduced", summary.messagesProduced)
            .withDetail("producerErrors", summary.producerErrors)
            .withDetail("successRate", String.format("%.2f%%", successRate))
            .build()
    }
}

/**
 * Kafka Consumer Health Indicator
 */
@Component("kafkaConsumerHealth")
class KafkaConsumerHealthIndicator(
    private val kafkaMetricsCollector: KafkaMetricsCollector,
    private val consumerLagMonitor: ConsumerLagMonitor
) : HealthIndicator {

    override fun health(): Health {
        val summary = kafkaMetricsCollector.getMetricsSummary()

        val successRate = if (summary.messagesConsumed > 0) {
            ((summary.messagesConsumed - summary.messagesFailed).toDouble() / summary.messagesConsumed) * 100
        } else {
            100.0
        }

        val criticalAlerts = consumerLagMonitor.getActiveAlerts(AlertLevel.CRITICAL)

        val builder = when {
            criticalAlerts.isNotEmpty() -> Health.down()
            successRate < 90 -> Health.down()
            successRate < 99 -> Health.status("DEGRADED")
            else -> Health.up()
        }

        return builder
            .withDetail("messagesConsumed", summary.messagesConsumed)
            .withDetail("messagesFailed", summary.messagesFailed)
            .withDetail("successRate", String.format("%.2f%%", successRate))
            .withDetail("totalLag", summary.totalConsumerLag)
            .withDetail("criticalLagAlerts", criticalAlerts.size)
            .build()
    }
}