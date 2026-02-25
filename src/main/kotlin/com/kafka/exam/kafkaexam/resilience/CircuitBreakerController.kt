package com.kafka.exam.kafkaexam.resilience

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

data class CircuitBreakerStatus(
    val name: String,
    val state: String,
    val metrics: CircuitBreakerMetrics
)

data class CircuitBreakerMetrics(
    val failureRate: Float,
    val slowCallRate: Float,
    val numberOfBufferedCalls: Int,
    val numberOfFailedCalls: Int,
    val numberOfSuccessfulCalls: Int,
    val numberOfSlowCalls: Int,
    val numberOfNotPermittedCalls: Long
)

@RestController
@RequestMapping("/api/circuit-breakers")
class CircuitBreakerController(
    private val circuitBreakerRegistry: CircuitBreakerRegistry
) {

    @GetMapping
    fun getAllCircuitBreakers(): ResponseEntity<List<CircuitBreakerStatus>> {
        val statuses = circuitBreakerRegistry.allCircuitBreakers.map { cb ->
            toStatus(cb)
        }
        return ResponseEntity.ok(statuses)
    }

    @GetMapping("/{name}")
    fun getCircuitBreaker(@PathVariable name: String): ResponseEntity<CircuitBreakerStatus> {
        return try {
            val cb = circuitBreakerRegistry.circuitBreaker(name)
            ResponseEntity.ok(toStatus(cb))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    @PostMapping("/{name}/reset")
    fun resetCircuitBreaker(@PathVariable name: String): ResponseEntity<CircuitBreakerStatus> {
        return try {
            val cb = circuitBreakerRegistry.circuitBreaker(name)
            cb.reset()
            ResponseEntity.ok(toStatus(cb))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    @PostMapping("/{name}/disable")
    fun disableCircuitBreaker(@PathVariable name: String): ResponseEntity<CircuitBreakerStatus> {
        return try {
            val cb = circuitBreakerRegistry.circuitBreaker(name)
            cb.transitionToDisabledState()
            ResponseEntity.ok(toStatus(cb))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    @PostMapping("/{name}/enable")
    fun enableCircuitBreaker(@PathVariable name: String): ResponseEntity<CircuitBreakerStatus> {
        return try {
            val cb = circuitBreakerRegistry.circuitBreaker(name)
            cb.transitionToClosedState()
            ResponseEntity.ok(toStatus(cb))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    @PostMapping("/{name}/force-open")
    fun forceOpenCircuitBreaker(@PathVariable name: String): ResponseEntity<CircuitBreakerStatus> {
        return try {
            val cb = circuitBreakerRegistry.circuitBreaker(name)
            cb.transitionToForcedOpenState()
            ResponseEntity.ok(toStatus(cb))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    private fun toStatus(cb: CircuitBreaker): CircuitBreakerStatus {
        val metrics = cb.metrics
        return CircuitBreakerStatus(
            name = cb.name,
            state = cb.state.name,
            metrics = CircuitBreakerMetrics(
                failureRate = metrics.failureRate,
                slowCallRate = metrics.slowCallRate,
                numberOfBufferedCalls = metrics.numberOfBufferedCalls,
                numberOfFailedCalls = metrics.numberOfFailedCalls,
                numberOfSuccessfulCalls = metrics.numberOfSuccessfulCalls,
                numberOfSlowCalls = metrics.numberOfSlowCalls,
                numberOfNotPermittedCalls = metrics.numberOfNotPermittedCalls
            )
        )
    }
}
