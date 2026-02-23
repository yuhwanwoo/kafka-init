package com.kafka.exam.kafkaexam.streams.topology

import com.kafka.exam.kafkaexam.streams.model.OrderEvent
import com.kafka.exam.kafkaexam.streams.model.OrderStatistics
import com.kafka.exam.kafkaexam.streams.serde.JsonSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OrderStatisticsTopology(
    @Value("\${kafka.streams.input-topic}") private val inputTopic: String,
    @Value("\${kafka.streams.output-topic}") private val outputTopic: String
) {

    private val log = LoggerFactory.getLogger(OrderStatisticsTopology::class.java)

    companion object {
        const val ORDER_STATISTICS_STORE = "order-statistics-store"
        const val HOURLY_STATISTICS_STORE = "hourly-statistics-store"
    }

    @Bean
    fun orderStatisticsStream(streamsBuilder: StreamsBuilder): KStream<String, OrderEvent> {
        val orderEventSerde = JsonSerde.create<OrderEvent>()
        val orderStatsSerde = JsonSerde.create<OrderStatistics>()

        // 1. Source: order-events 토픽에서 주문 이벤트 읽기
        val orderEvents: KStream<String, OrderEvent> = streamsBuilder
            .stream(inputTopic, Consumed.with(Serdes.String(), orderEventSerde))
            .peek { key, value -> log.info("Received order event: key={}, orderId={}", key, value.orderId) }

        // 2. 상품별로 그룹화하여 통계 집계 (KTable)
        val productStatistics: KTable<String, OrderStatistics> = orderEvents
            .groupBy(
                { _, event -> event.productId },
                Grouped.with(Serdes.String(), orderEventSerde)
            )
            .aggregate(
                { OrderStatistics.empty("", "") },
                { productId, event, stats ->
                    val updatedStats = if (stats.productId.isEmpty()) {
                        OrderStatistics.empty(productId, event.productName)
                    } else {
                        stats
                    }.aggregate(event)
                    log.info("Updated statistics for product {}: orders={}, revenue={}",
                        productId, updatedStats.totalOrders, updatedStats.totalRevenue)
                    updatedStats
                },
                Materialized.`as`<String, OrderStatistics, KeyValueStore<Bytes, ByteArray>>(ORDER_STATISTICS_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(orderStatsSerde)
            )

        // 3. 통계 변경사항을 output 토픽으로 발행
        productStatistics
            .toStream()
            .peek { key, value -> log.info("Publishing statistics: productId={}, totalOrders={}", key, value.totalOrders) }
            .to(outputTopic, Produced.with(Serdes.String(), orderStatsSerde))

        return orderEvents
    }

    @Bean
    fun hourlyOrderCountStream(streamsBuilder: StreamsBuilder): KTable<Windowed<String>, Long> {
        val orderEventSerde = JsonSerde.create<OrderEvent>()

        // 시간별 주문 수 집계 (1시간 윈도우)
        val hourlyOrderCounts = streamsBuilder
            .stream(inputTopic, Consumed.with(Serdes.String(), orderEventSerde))
            .groupBy(
                { _, event -> event.productId },
                Grouped.with(Serdes.String(), orderEventSerde)
            )
            .windowedBy(TimeWindows.ofSizeWithNoGrace(java.time.Duration.ofHours(1)))
            .count(
                Materialized.`as`<String, Long, org.apache.kafka.streams.state.WindowStore<Bytes, ByteArray>>(HOURLY_STATISTICS_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
            )

        return hourlyOrderCounts
    }
}