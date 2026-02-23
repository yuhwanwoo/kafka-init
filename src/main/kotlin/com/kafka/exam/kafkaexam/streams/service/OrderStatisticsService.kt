package com.kafka.exam.kafkaexam.streams.service

import com.kafka.exam.kafkaexam.streams.model.OrderStatistics
import com.kafka.exam.kafkaexam.streams.topology.OrderStatisticsTopology
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.slf4j.LoggerFactory
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class OrderStatisticsService(
    private val streamsBuilderFactoryBean: StreamsBuilderFactoryBean
) {

    private val log = LoggerFactory.getLogger(OrderStatisticsService::class.java)

    private fun getStatisticsStore(): ReadOnlyKeyValueStore<String, OrderStatistics>? {
        val kafkaStreams = streamsBuilderFactoryBean.kafkaStreams ?: return null
        if (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            log.warn("Kafka Streams is not running. Current state: {}", kafkaStreams.state())
            return null
        }

        return kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                OrderStatisticsTopology.ORDER_STATISTICS_STORE,
                QueryableStoreTypes.keyValueStore()
            )
        )
    }

    private fun getHourlyStore(): ReadOnlyWindowStore<String, Long>? {
        val kafkaStreams = streamsBuilderFactoryBean.kafkaStreams ?: return null
        if (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            return null
        }

        return kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                OrderStatisticsTopology.HOURLY_STATISTICS_STORE,
                QueryableStoreTypes.windowStore()
            )
        )
    }

    fun getStatisticsByProductId(productId: String): OrderStatistics? {
        val store = getStatisticsStore() ?: return null
        return store.get(productId)
    }

    fun getAllStatistics(): List<OrderStatistics> {
        val store = getStatisticsStore() ?: return emptyList()
        val statistics = mutableListOf<OrderStatistics>()
        store.all().use { iterator ->
            iterator.forEach { keyValue ->
                statistics.add(keyValue.value)
            }
        }
        return statistics
    }

    fun getTopProducts(limit: Int = 10): List<OrderStatistics> {
        return getAllStatistics()
            .sortedByDescending { it.totalRevenue }
            .take(limit)
    }

    fun getHourlyOrderCount(productId: String, fromTime: Instant, toTime: Instant): Map<Instant, Long> {
        val store = getHourlyStore() ?: return emptyMap()
        val result = mutableMapOf<Instant, Long>()

        store.fetch(productId, fromTime, toTime).use { iterator ->
            iterator.forEach { keyValue ->
                result[Instant.ofEpochMilli(keyValue.key)] = keyValue.value
            }
        }

        return result
    }

    fun getStreamsState(): String {
        val kafkaStreams = streamsBuilderFactoryBean.kafkaStreams
        return kafkaStreams?.state()?.name ?: "NOT_INITIALIZED"
    }
}