package com.kafka.exam.kafkaexam.consumer.dlt

import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

@Service
@Transactional(readOnly = true)
class DltDashboardService(
    private val failedMessageRepository: FailedMessageRepository,
    private val dltAnalysisService: DltAnalysisService,
    private val dltReprocessService: DltReprocessService
) {

    /**
     * 대시보드 전체 요약 정보
     */
    fun getDashboardSummary(): DashboardSummary {
        val stats = dltReprocessService.getStatistics()
        val topicAnalysis = dltAnalysisService.analyzeByTopic()
        val retryEfficiency = dltAnalysisService.analyzeRetryEfficiency()

        // 최근 1시간 추세
        val recentTrend = dltAnalysisService.analyzeHourlyTrend(1)
        val recentCount = recentTrend.sumOf { it.count }

        // 알림 레벨 결정
        val alertLevel = determineAlertLevel(stats.pending.toInt(), recentCount)

        return DashboardSummary(
            statistics = stats,
            topicSummary = topicAnalysis.take(5),
            retrySuccessRate = retryEfficiency.successRate,
            recentHourCount = recentCount,
            alertLevel = alertLevel,
            updatedAt = LocalDateTime.now()
        )
    }

    /**
     * 실시간 현황 (경량 조회)
     */
    fun getRealTimeStatus(): RealTimeStatus {
        val pendingCount = failedMessageRepository.countByStatus(FailedMessageStatus.PENDING)
        val recentMessages = failedMessageRepository.findByStatus(
            FailedMessageStatus.PENDING,
            org.springframework.data.domain.PageRequest.of(0, 10)
        )

        return RealTimeStatus(
            pendingCount = pendingCount,
            recentMessages = recentMessages.content.map { it.toSummary() },
            timestamp = LocalDateTime.now()
        )
    }

    /**
     * 상세 대시보드 (전체 분석)
     */
    fun getDetailedDashboard(): DetailedDashboard {
        val summary = getDashboardSummary()
        val errorPatterns = dltAnalysisService.analyzeErrorPatterns()
        val hourlyTrend = dltAnalysisService.analyzeHourlyTrend(24)
        val topicAnalysis = dltAnalysisService.analyzeByTopic()
        val retryAnalysis = dltAnalysisService.analyzeRetryEfficiency()

        return DetailedDashboard(
            summary = summary,
            errorPatterns = errorPatterns,
            hourlyTrend = hourlyTrend,
            topicAnalysis = topicAnalysis,
            retryAnalysis = retryAnalysis,
            generatedAt = LocalDateTime.now()
        )
    }

    /**
     * 토픽별 상세 현황
     */
    fun getTopicDetail(topic: String): TopicDetail {
        val pendingMessages = failedMessageRepository.findByOriginalTopicAndStatus(
            topic, FailedMessageStatus.PENDING
        )
        val retriedMessages = failedMessageRepository.findByOriginalTopicAndStatus(
            topic, FailedMessageStatus.RETRIED
        )

        val errorTypes = pendingMessages
            .groupBy { extractErrorType(it.errorMessage) }
            .mapValues { it.value.size }

        return TopicDetail(
            topic = topic,
            pendingCount = pendingMessages.size,
            retriedCount = retriedMessages.size,
            errorTypeDistribution = errorTypes,
            recentMessages = pendingMessages.take(10).map { it.toSummary() },
            avgRetryCount = pendingMessages.map { it.retryCount }.average().takeIf { !it.isNaN() } ?: 0.0
        )
    }

    /**
     * 헬스체크 정보
     */
    fun getHealthStatus(): DltHealthStatus {
        val stats = dltReprocessService.getStatistics()
        val pendingCount = stats.pending.toInt()

        // 최근 10분간 추가된 메시지 수
        val tenMinutesAgo = LocalDateTime.now().minusMinutes(10)
        val recentMessages = failedMessageRepository.findByCreatedAtBetweenAndStatus(
            tenMinutesAgo, LocalDateTime.now(), FailedMessageStatus.PENDING
        )

        val status = when {
            pendingCount > 1000 -> HealthLevel.CRITICAL
            pendingCount > 100 -> HealthLevel.WARNING
            recentMessages.size > 50 -> HealthLevel.WARNING
            else -> HealthLevel.HEALTHY
        }

        return DltHealthStatus(
            status = status,
            pendingCount = pendingCount,
            recentAddedCount = recentMessages.size,
            message = getHealthMessage(status, pendingCount, recentMessages.size),
            checkedAt = LocalDateTime.now()
        )
    }

    private fun determineAlertLevel(pendingCount: Int, recentCount: Int): AlertLevel {
        return when {
            pendingCount > 1000 || recentCount > 100 -> AlertLevel.CRITICAL
            pendingCount > 100 || recentCount > 20 -> AlertLevel.WARNING
            pendingCount > 10 -> AlertLevel.INFO
            else -> AlertLevel.NORMAL
        }
    }

    private fun extractErrorType(errorMessage: String?): String {
        if (errorMessage.isNullOrBlank()) return "Unknown"
        val firstLine = errorMessage.lines().firstOrNull() ?: return "Unknown"
        val exceptionMatch = Regex("([A-Z][a-zA-Z]*Exception|[A-Z][a-zA-Z]*Error)").find(firstLine)
        return exceptionMatch?.value ?: "Other"
    }

    private fun getHealthMessage(status: HealthLevel, pending: Int, recent: Int): String {
        return when (status) {
            HealthLevel.CRITICAL -> "Critical: $pending pending messages, $recent added in last 10 min"
            HealthLevel.WARNING -> "Warning: $pending pending messages need attention"
            HealthLevel.HEALTHY -> "Healthy: DLT queue is under control"
        }
    }

    private fun FailedMessage.toSummary() = FailedMessageSummary(
        id = id ?: 0,
        originalTopic = originalTopic,
        errorType = extractErrorType(errorMessage),
        retryCount = retryCount,
        createdAt = createdAt
    )
}

// Dashboard DTOs

data class DashboardSummary(
    val statistics: DltStatistics,
    val topicSummary: List<TopicFailureAnalysis>,
    val retrySuccessRate: Double,
    val recentHourCount: Int,
    val alertLevel: AlertLevel,
    val updatedAt: LocalDateTime
)

enum class AlertLevel {
    NORMAL, INFO, WARNING, CRITICAL
}

data class RealTimeStatus(
    val pendingCount: Long,
    val recentMessages: List<FailedMessageSummary>,
    val timestamp: LocalDateTime
)

data class FailedMessageSummary(
    val id: Long,
    val originalTopic: String,
    val errorType: String,
    val retryCount: Int,
    val createdAt: LocalDateTime
)

data class DetailedDashboard(
    val summary: DashboardSummary,
    val errorPatterns: ErrorPatternAnalysis,
    val hourlyTrend: List<HourlyTrend>,
    val topicAnalysis: List<TopicFailureAnalysis>,
    val retryAnalysis: RetryEfficiencyAnalysis,
    val generatedAt: LocalDateTime
)

data class TopicDetail(
    val topic: String,
    val pendingCount: Int,
    val retriedCount: Int,
    val errorTypeDistribution: Map<String, Int>,
    val recentMessages: List<FailedMessageSummary>,
    val avgRetryCount: Double
)

enum class HealthLevel {
    HEALTHY, WARNING, CRITICAL
}

data class DltHealthStatus(
    val status: HealthLevel,
    val pendingCount: Int,
    val recentAddedCount: Int,
    val message: String,
    val checkedAt: LocalDateTime
)
