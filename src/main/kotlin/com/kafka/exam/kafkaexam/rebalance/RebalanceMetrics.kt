package com.kafka.exam.kafkaexam.rebalance

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Rebalance 메트릭 수집
 */
class RebalanceMetrics private constructor() {

    private val totalRebalances = AtomicInteger(0)
    private val totalRevocations = AtomicInteger(0)
    private val totalAssignments = AtomicInteger(0)
    private val totalLostPartitions = AtomicInteger(0)
    private val totalErrors = AtomicInteger(0)

    private val revokedPartitions = AtomicLong(0)
    private val assignedPartitions = AtomicLong(0)
    private val lostPartitions = AtomicLong(0)

    private val rebalanceHistory = ConcurrentLinkedQueue<RebalanceEvent>()
    private val maxHistorySize = 100

    companion object {
        val instance: RebalanceMetrics by lazy { RebalanceMetrics() }
    }

    fun recordRevocation(partitionCount: Int) {
        totalRebalances.incrementAndGet()
        totalRevocations.incrementAndGet()
        revokedPartitions.addAndGet(partitionCount.toLong())

        addToHistory(RebalanceEvent(
            type = RebalanceEventType.REVOCATION,
            partitionCount = partitionCount,
            timestamp = Instant.now()
        ))
    }

    fun recordAssignment(partitionCount: Int) {
        totalAssignments.incrementAndGet()
        assignedPartitions.addAndGet(partitionCount.toLong())

        addToHistory(RebalanceEvent(
            type = RebalanceEventType.ASSIGNMENT,
            partitionCount = partitionCount,
            timestamp = Instant.now()
        ))
    }

    fun recordLost(partitionCount: Int) {
        totalLostPartitions.incrementAndGet()
        lostPartitions.addAndGet(partitionCount.toLong())

        addToHistory(RebalanceEvent(
            type = RebalanceEventType.LOST,
            partitionCount = partitionCount,
            timestamp = Instant.now()
        ))
    }

    fun recordError(phase: String) {
        totalErrors.incrementAndGet()

        addToHistory(RebalanceEvent(
            type = RebalanceEventType.ERROR,
            partitionCount = 0,
            timestamp = Instant.now(),
            errorPhase = phase
        ))
    }

    private fun addToHistory(event: RebalanceEvent) {
        rebalanceHistory.add(event)
        while (rebalanceHistory.size > maxHistorySize) {
            rebalanceHistory.poll()
        }
    }

    fun getStats(): RebalanceStats {
        return RebalanceStats(
            totalRebalances = totalRebalances.get(),
            totalRevocations = totalRevocations.get(),
            totalAssignments = totalAssignments.get(),
            totalLostPartitions = totalLostPartitions.get(),
            totalErrors = totalErrors.get(),
            revokedPartitions = revokedPartitions.get(),
            assignedPartitions = assignedPartitions.get(),
            lostPartitions = lostPartitions.get()
        )
    }

    fun getHistory(): List<RebalanceEvent> {
        return rebalanceHistory.toList()
    }

    fun getRecentHistory(count: Int): List<RebalanceEvent> {
        return rebalanceHistory.toList().takeLast(count)
    }

    fun reset() {
        totalRebalances.set(0)
        totalRevocations.set(0)
        totalAssignments.set(0)
        totalLostPartitions.set(0)
        totalErrors.set(0)
        revokedPartitions.set(0)
        assignedPartitions.set(0)
        lostPartitions.set(0)
        rebalanceHistory.clear()
    }
}

data class RebalanceStats(
    val totalRebalances: Int,
    val totalRevocations: Int,
    val totalAssignments: Int,
    val totalLostPartitions: Int,
    val totalErrors: Int,
    val revokedPartitions: Long,
    val assignedPartitions: Long,
    val lostPartitions: Long
)

data class RebalanceEvent(
    val type: RebalanceEventType,
    val partitionCount: Int,
    val timestamp: Instant,
    val errorPhase: String? = null
)

enum class RebalanceEventType {
    REVOCATION,
    ASSIGNMENT,
    LOST,
    ERROR
}