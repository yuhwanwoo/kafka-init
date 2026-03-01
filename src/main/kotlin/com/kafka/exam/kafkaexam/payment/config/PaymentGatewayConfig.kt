package com.kafka.exam.kafkaexam.payment.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "payment.gateway")
class PaymentGatewayConfig {
    var baseUrl: String = "https://api.payment-gateway.com"
    var apiKey: String = ""
    var secretKey: String = ""
    var simulationMode: Boolean = true
    var timeoutMs: Long = 5000
    var retryCount: Int = 3

    // 시뮬레이션 모드 설정
    var simulation: SimulationConfig = SimulationConfig()

    class SimulationConfig {
        var delayMs: Long = 100
        var failureRate: Double = 0.0  // 0.0 ~ 1.0 (실패 확률)
        var slowCallRate: Double = 0.0  // 0.0 ~ 1.0 (느린 호출 확률)
        var slowCallDelayMs: Long = 3000
    }
}