package com.kafka.exam.kafkaexam.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class ResilienceConfig {

    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        const val PAYMENT_CIRCUIT_BREAKER = "paymentCircuitBreaker"
        const val KAFKA_CIRCUIT_BREAKER = "kafkaCircuitBreaker"
        const val INVENTORY_CIRCUIT_BREAKER = "inventoryCircuitBreaker"
    }

    @Bean
    fun circuitBreakerRegistry(): CircuitBreakerRegistry {
        val defaultConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .slowCallDurationThreshold(Duration.ofSeconds(2))
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .permittedNumberOfCallsInHalfOpenState(3)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .build()

        val paymentConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(50f)
            .slowCallRateThreshold(80f)
            .slowCallDurationThreshold(Duration.ofSeconds(3))
            .waitDurationInOpenState(Duration.ofSeconds(60))
            .permittedNumberOfCallsInHalfOpenState(3)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .recordExceptions(Exception::class.java)
            .ignoreExceptions(IllegalArgumentException::class.java)
            .build()

        val kafkaConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(70f)
            .slowCallRateThreshold(90f)
            .slowCallDurationThreshold(Duration.ofSeconds(5))
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .permittedNumberOfCallsInHalfOpenState(5)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .minimumNumberOfCalls(10)
            .build()

        val registry = CircuitBreakerRegistry.of(defaultConfig)

        registry.addConfiguration(PAYMENT_CIRCUIT_BREAKER, paymentConfig)
        registry.addConfiguration(KAFKA_CIRCUIT_BREAKER, kafkaConfig)

        return registry
    }

    @Bean
    fun paymentCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker {
        val circuitBreaker = registry.circuitBreaker(PAYMENT_CIRCUIT_BREAKER)
        registerEventListeners(circuitBreaker, PAYMENT_CIRCUIT_BREAKER)
        return circuitBreaker
    }

    @Bean
    fun kafkaCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker {
        val circuitBreaker = registry.circuitBreaker(KAFKA_CIRCUIT_BREAKER)
        registerEventListeners(circuitBreaker, KAFKA_CIRCUIT_BREAKER)
        return circuitBreaker
    }

    @Bean
    fun inventoryCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker {
        val circuitBreaker = registry.circuitBreaker(INVENTORY_CIRCUIT_BREAKER)
        registerEventListeners(circuitBreaker, INVENTORY_CIRCUIT_BREAKER)
        return circuitBreaker
    }

    private fun registerEventListeners(circuitBreaker: CircuitBreaker, name: String) {
        circuitBreaker.eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                log.warn(
                    "CircuitBreaker [{}] 상태 변경: {} -> {}",
                    name,
                    event.stateTransition.fromState,
                    event.stateTransition.toState
                )
            }
            .onFailureRateExceeded { event ->
                log.error(
                    "CircuitBreaker [{}] 실패율 초과: {}%",
                    name,
                    event.failureRate
                )
            }
            .onSlowCallRateExceeded { event ->
                log.warn(
                    "CircuitBreaker [{}] 느린 호출 비율 초과: {}%",
                    name,
                    event.slowCallRate
                )
            }
    }
}