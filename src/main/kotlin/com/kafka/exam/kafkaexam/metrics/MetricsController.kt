package com.kafka.exam.kafkaexam.metrics

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/metrics")
class MetricsController(
    private val kafkaMetricsCollector: KafkaMetricsCollector,
    private val consumerLagMonitor: ConsumerLagMonitor
) {
    /**
     * 전체 메트릭 요약
     */
    @GetMapping
    fun getMetricsSummary(): ResponseEntity<MetricsSummary> {
        return ResponseEntity.ok(kafkaMetricsCollector.getMetricsSummary())
    }

    /**
     * 메트릭 리셋
     */
    @DeleteMapping
    fun resetMetrics(): ResponseEntity<Map<String, String>> {
        kafkaMetricsCollector.resetMetrics()
        return ResponseEntity.ok(mapOf("message" to "Metrics reset successfully"))
    }

    // ==================== Consumer Lag ====================

    /**
     * 모든 Consumer Group의 Lag 조회
     */
    @GetMapping("/lag")
    fun getAllLags(): ResponseEntity<List<GroupLagInfo>> {
        return ResponseEntity.ok(consumerLagMonitor.getAllGroupLags())
    }

    /**
     * 특정 Consumer Group의 Lag 조회
     */
    @GetMapping("/lag/{groupId}")
    fun getGroupLag(@PathVariable groupId: String): ResponseEntity<GroupLagInfo> {
        val lagInfo = consumerLagMonitor.collectGroupLag(groupId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(lagInfo)
    }

    /**
     * Lag 트렌드 분석
     */
    @GetMapping("/lag/{groupId}/trend")
    fun getLagTrend(@PathVariable groupId: String): ResponseEntity<LagTrend> {
        val trend = consumerLagMonitor.analyzeTrend(groupId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(trend)
    }

    // ==================== Alerts ====================

    /**
     * 모든 알림 상태 조회
     */
    @GetMapping("/alerts")
    fun getAlerts(): ResponseEntity<Map<String, AlertLevel>> {
        return ResponseEntity.ok(consumerLagMonitor.getAlertStates())
    }

    /**
     * 활성 알림만 조회 (WARNING 이상)
     */
    @GetMapping("/alerts/active")
    fun getActiveAlerts(
        @RequestParam(defaultValue = "WARNING") minLevel: AlertLevel
    ): ResponseEntity<Map<String, AlertLevel>> {
        return ResponseEntity.ok(consumerLagMonitor.getActiveAlerts(minLevel))
    }

    // ==================== Dashboard Data ====================

    /**
     * 대시보드용 통합 데이터
     */
    @GetMapping("/dashboard")
    fun getDashboardData(): ResponseEntity<DashboardData> {
        val metricsSummary = kafkaMetricsCollector.getMetricsSummary()
        val allLags = consumerLagMonitor.getAllGroupLags()
        val alerts = consumerLagMonitor.getActiveAlerts()

        val producerSuccessRate = if (metricsSummary.messagesProduced > 0) {
            ((metricsSummary.messagesProduced - metricsSummary.producerErrors).toDouble() / metricsSummary.messagesProduced) * 100
        } else 100.0

        val consumerSuccessRate = if (metricsSummary.messagesConsumed > 0) {
            ((metricsSummary.messagesConsumed - metricsSummary.messagesFailed).toDouble() / metricsSummary.messagesConsumed) * 100
        } else 100.0

        return ResponseEntity.ok(
            DashboardData(
                producer = ProducerStats(
                    messagesProduced = metricsSummary.messagesProduced,
                    errors = metricsSummary.producerErrors,
                    successRate = producerSuccessRate
                ),
                consumer = ConsumerStats(
                    messagesConsumed = metricsSummary.messagesConsumed,
                    messagesFailed = metricsSummary.messagesFailed,
                    successRate = consumerSuccessRate,
                    totalLag = metricsSummary.totalConsumerLag
                ),
                consumerGroups = allLags.map { lagInfo ->
                    ConsumerGroupStats(
                        groupId = lagInfo.groupId,
                        totalLag = lagInfo.totalLag,
                        partitionCount = lagInfo.partitionLags.size,
                        maxPartitionLag = lagInfo.partitionLags.maxOfOrNull { it.lag } ?: 0
                    )
                },
                alerts = AlertStats(
                    total = alerts.size,
                    critical = alerts.count { it.value == AlertLevel.CRITICAL },
                    warning = alerts.count { it.value == AlertLevel.WARNING }
                ),
                health = determineOverallHealth(
                    producerSuccessRate,
                    consumerSuccessRate,
                    alerts.count { it.value == AlertLevel.CRITICAL }
                )
            )
        )
    }

    private fun determineOverallHealth(
        producerRate: Double,
        consumerRate: Double,
        criticalAlerts: Int
    ): HealthStatus {
        return when {
            criticalAlerts > 0 -> HealthStatus.CRITICAL
            producerRate < 90 || consumerRate < 90 -> HealthStatus.CRITICAL
            producerRate < 99 || consumerRate < 99 -> HealthStatus.WARNING
            else -> HealthStatus.HEALTHY
        }
    }
}

// ==================== DTOs ====================

data class DashboardData(
    val producer: ProducerStats,
    val consumer: ConsumerStats,
    val consumerGroups: List<ConsumerGroupStats>,
    val alerts: AlertStats,
    val health: HealthStatus
)

data class ProducerStats(
    val messagesProduced: Long,
    val errors: Long,
    val successRate: Double
)

data class ConsumerStats(
    val messagesConsumed: Long,
    val messagesFailed: Long,
    val successRate: Double,
    val totalLag: Long
)

data class ConsumerGroupStats(
    val groupId: String,
    val totalLag: Long,
    val partitionCount: Int,
    val maxPartitionLag: Long
)

data class AlertStats(
    val total: Int,
    val critical: Int,
    val warning: Int
)

enum class HealthStatus {
    HEALTHY, WARNING, CRITICAL
}