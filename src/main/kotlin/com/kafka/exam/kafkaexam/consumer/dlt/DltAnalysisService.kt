package com.kafka.exam.kafkaexam.consumer.dlt

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

@Service
@Transactional(readOnly = true)
class DltAnalysisService(
    private val failedMessageRepository: FailedMessageRepository
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 에러 유형별 그룹핑 분석
     */
    fun analyzeErrorPatterns(): ErrorPatternAnalysis {
        val allMessages = failedMessageRepository.findByStatus(FailedMessageStatus.PENDING)

        val errorGroups = allMessages
            .groupBy { extractErrorType(it.errorMessage) }
            .map { (errorType, messages) ->
                ErrorGroup(
                    errorType = errorType,
                    count = messages.size,
                    affectedTopics = messages.map { it.originalTopic }.distinct(),
                    firstOccurrence = messages.minOfOrNull { it.createdAt },
                    lastOccurrence = messages.maxOfOrNull { it.createdAt },
                    sampleMessageIds = messages.take(5).mapNotNull { it.id }
                )
            }
            .sortedByDescending { it.count }

        return ErrorPatternAnalysis(
            totalPending = allMessages.size,
            uniqueErrorTypes = errorGroups.size,
            errorGroups = errorGroups
        )
    }

    /**
     * 토픽별 실패 분석
     */
    fun analyzeByTopic(): List<TopicFailureAnalysis> {
        val stats = failedMessageRepository.getStatisticsByTopicAndStatus()
        val topicMap = mutableMapOf<String, TopicFailureAnalysis>()

        stats.forEach { row ->
            val topic = row[0] as String
            val status = row[1] as FailedMessageStatus
            val count = (row[2] as Long).toInt()

            val analysis = topicMap.getOrPut(topic) {
                TopicFailureAnalysis(topic = topic)
            }

            when (status) {
                FailedMessageStatus.PENDING -> analysis.pending = count
                FailedMessageStatus.RETRIED -> analysis.retried = count
                FailedMessageStatus.RESOLVED -> analysis.resolved = count
                FailedMessageStatus.IGNORED -> analysis.ignored = count
            }
        }

        return topicMap.values.toList().sortedByDescending { it.total }
    }

    /**
     * 시간대별 실패 트렌드 분석
     */
    fun analyzeHourlyTrend(hours: Int = 24): List<HourlyTrend> {
        val now = LocalDateTime.now()
        val startTime = now.minusHours(hours.toLong())

        val pendingMessages = failedMessageRepository.findByCreatedAtBetweenAndStatus(
            startTime, now, FailedMessageStatus.PENDING
        )

        val hourlyGroups = pendingMessages.groupBy { message ->
            message.createdAt.truncatedTo(ChronoUnit.HOURS)
        }

        return (0 until hours).map { hoursAgo ->
            val hour = now.minusHours(hoursAgo.toLong()).truncatedTo(ChronoUnit.HOURS)
            val count = hourlyGroups[hour]?.size ?: 0
            HourlyTrend(
                hour = hour,
                count = count
            )
        }.reversed()
    }

    /**
     * 재시도 효율성 분석
     */
    fun analyzeRetryEfficiency(): RetryEfficiencyAnalysis {
        val allMessages = failedMessageRepository.findAll()

        val retriedMessages = allMessages.filter { it.status == FailedMessageStatus.RETRIED }
        val pendingWithRetries = allMessages.filter {
            it.status == FailedMessageStatus.PENDING && it.retryCount > 0
        }

        val avgRetryCountForSuccess = if (retriedMessages.isNotEmpty()) {
            retriedMessages.map { it.retryCount }.average()
        } else 0.0

        val retryDistribution = allMessages
            .groupBy { it.retryCount }
            .mapValues { it.value.size }
            .toSortedMap()

        return RetryEfficiencyAnalysis(
            totalRetried = retriedMessages.size,
            pendingAfterRetry = pendingWithRetries.size,
            avgRetriesForSuccess = avgRetryCountForSuccess,
            retryDistribution = retryDistribution,
            successRate = if (retriedMessages.size + pendingWithRetries.size > 0) {
                retriedMessages.size.toDouble() / (retriedMessages.size + pendingWithRetries.size) * 100
            } else 0.0
        )
    }

    /**
     * 에러 메시지에서 에러 유형 추출
     */
    private fun extractErrorType(errorMessage: String?): String {
        if (errorMessage.isNullOrBlank()) return "Unknown"

        // 일반적인 예외 패턴 매칭
        val patterns = listOf(
            "NullPointerException" to "NullPointerException",
            "IllegalArgumentException" to "IllegalArgumentException",
            "IllegalStateException" to "IllegalStateException",
            "JsonParseException" to "JSON Parse Error",
            "JsonMappingException" to "JSON Mapping Error",
            "DeserializationException" to "Deserialization Error",
            "SerializationException" to "Serialization Error",
            "TimeoutException" to "Timeout",
            "ConnectionException" to "Connection Error",
            "SQLException" to "Database Error",
            "DataIntegrityViolationException" to "Data Integrity Error",
            "ConstraintViolationException" to "Constraint Violation",
            "OptimisticLockingFailureException" to "Optimistic Locking",
            "OutOfMemoryError" to "Out of Memory",
            "StackOverflowError" to "Stack Overflow"
        )

        for ((pattern, errorType) in patterns) {
            if (errorMessage.contains(pattern, ignoreCase = true)) {
                return errorType
            }
        }

        // 첫 줄에서 예외 클래스명 추출 시도
        val firstLine = errorMessage.lines().firstOrNull() ?: return "Unknown"
        val exceptionMatch = Regex("([A-Z][a-zA-Z]*Exception|[A-Z][a-zA-Z]*Error)").find(firstLine)

        return exceptionMatch?.value ?: "Other Error"
    }
}

// Analysis DTOs

data class ErrorPatternAnalysis(
    val totalPending: Int,
    val uniqueErrorTypes: Int,
    val errorGroups: List<ErrorGroup>
)

data class ErrorGroup(
    val errorType: String,
    val count: Int,
    val affectedTopics: List<String>,
    val firstOccurrence: LocalDateTime?,
    val lastOccurrence: LocalDateTime?,
    val sampleMessageIds: List<Long>
)

data class TopicFailureAnalysis(
    val topic: String,
    var pending: Int = 0,
    var retried: Int = 0,
    var resolved: Int = 0,
    var ignored: Int = 0
) {
    val total: Int get() = pending + retried + resolved + ignored
    val pendingRate: Double get() = if (total > 0) pending.toDouble() / total * 100 else 0.0
}

data class HourlyTrend(
    val hour: LocalDateTime,
    val count: Int
)

data class RetryEfficiencyAnalysis(
    val totalRetried: Int,
    val pendingAfterRetry: Int,
    val avgRetriesForSuccess: Double,
    val retryDistribution: Map<Int, Int>,
    val successRate: Double
)
