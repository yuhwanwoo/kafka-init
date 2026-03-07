package com.kafka.exam.kafkaexam.interceptor

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class InterceptorMetrics private constructor() {

    private val consumedByTopic = ConcurrentHashMap<String, AtomicLong>()
    private val sentByTopic = ConcurrentHashMap<String, AtomicLong>()
    private val acknowledgedByTopic = ConcurrentHashMap<String, AtomicLong>()
    private val failedByTopic = ConcurrentHashMap<String, AtomicLong>()
    private val commitsByTopic = ConcurrentHashMap<String, AtomicLong>()

    private val totalLatency = AtomicLong(0)
    private val latencyCount = AtomicLong(0)
    private val maxLatency = AtomicLong(0)
    private val minLatency = AtomicLong(Long.MAX_VALUE)

    companion object {
        val instance: InterceptorMetrics by lazy { InterceptorMetrics() }
    }

    // Consumer 메트릭
    fun recordConsumed(topic: String, count: Int) {
        consumedByTopic.computeIfAbsent(topic) { AtomicLong(0) }
            .addAndGet(count.toLong())
    }

    fun recordLatency(topic: String, latencyMs: Long) {
        totalLatency.addAndGet(latencyMs)
        latencyCount.incrementAndGet()

        maxLatency.updateAndGet { current -> maxOf(current, latencyMs) }
        minLatency.updateAndGet { current -> minOf(current, latencyMs) }
    }

    fun recordCommit(topic: String) {
        commitsByTopic.computeIfAbsent(topic) { AtomicLong(0) }
            .incrementAndGet()
    }

    // Producer 메트릭
    fun recordSent(topic: String) {
        sentByTopic.computeIfAbsent(topic) { AtomicLong(0) }
            .incrementAndGet()
    }

    fun recordAcknowledged(topic: String) {
        acknowledgedByTopic.computeIfAbsent(topic) { AtomicLong(0) }
            .incrementAndGet()
    }

    fun recordFailed(topic: String) {
        failedByTopic.computeIfAbsent(topic) { AtomicLong(0) }
            .incrementAndGet()
    }

    // 통계 조회
    fun getStats(): InterceptorStats {
        val count = latencyCount.get()
        return InterceptorStats(
            totalConsumed = consumedByTopic.values.sumOf { it.get() },
            totalSent = sentByTopic.values.sumOf { it.get() },
            totalAcknowledged = acknowledgedByTopic.values.sumOf { it.get() },
            totalFailed = failedByTopic.values.sumOf { it.get() },
            totalCommits = commitsByTopic.values.sumOf { it.get() },
            avgLatencyMs = if (count > 0) totalLatency.get() / count else 0,
            maxLatencyMs = if (maxLatency.get() == 0L) 0 else maxLatency.get(),
            minLatencyMs = if (minLatency.get() == Long.MAX_VALUE) 0 else minLatency.get(),
            consumedByTopic = consumedByTopic.mapValues { it.value.get() },
            sentByTopic = sentByTopic.mapValues { it.value.get() },
            failedByTopic = failedByTopic.mapValues { it.value.get() }
        )
    }

    fun getTopicStats(topic: String): TopicStats {
        return TopicStats(
            topic = topic,
            consumed = consumedByTopic[topic]?.get() ?: 0,
            sent = sentByTopic[topic]?.get() ?: 0,
            acknowledged = acknowledgedByTopic[topic]?.get() ?: 0,
            failed = failedByTopic[topic]?.get() ?: 0,
            commits = commitsByTopic[topic]?.get() ?: 0
        )
    }

    fun reset() {
        consumedByTopic.clear()
        sentByTopic.clear()
        acknowledgedByTopic.clear()
        failedByTopic.clear()
        commitsByTopic.clear()
        totalLatency.set(0)
        latencyCount.set(0)
        maxLatency.set(0)
        minLatency.set(Long.MAX_VALUE)
    }
}

data class InterceptorStats(
    val totalConsumed: Long,
    val totalSent: Long,
    val totalAcknowledged: Long,
    val totalFailed: Long,
    val totalCommits: Long,
    val avgLatencyMs: Long,
    val maxLatencyMs: Long,
    val minLatencyMs: Long,
    val consumedByTopic: Map<String, Long>,
    val sentByTopic: Map<String, Long>,
    val failedByTopic: Map<String, Long>
)

data class TopicStats(
    val topic: String,
    val consumed: Long,
    val sent: Long,
    val acknowledged: Long,
    val failed: Long,
    val commits: Long
)