package com.kafka.exam.kafkaexam.tracing

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

/**
 * 추적 데모용 Consumer.
 *
 * TracingRecordInterceptor가 리스너 호출 직전에 traceparent 헤더를 추출하여
 * CONSUMER span을 시작하므로, 이 안에서 tracer.withSpan을 호출하면
 * 자식 span으로 기록됩니다.
 */
@Component
class TracingDemoConsumer(
    private val tracer: Tracer
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["tracing-demo-topic"],
        groupId = "\${spring.kafka.consumer.group-id}-tracing-demo"
    )
    fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        tracer.withSpan("business.handle-order-event", SpanKind.INTERNAL) { span ->
            span.setAttribute("order.key", record.key() ?: "null")
            log.info(
                "[TRACE-DEMO] 소비 - topic={}, key={}, value={}, traceId={}",
                record.topic(), record.key(), record.value(), span.traceId
            )
            // 비즈니스 로직 시뮬레이션
            Thread.sleep(20)
        }
        ack.acknowledge()
    }
}