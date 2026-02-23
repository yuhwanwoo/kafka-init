package com.kafka.exam.kafkaexam.streams.serde

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

class JsonSerde<T>(private val clazz: Class<T>) : Serde<T> {

    private val objectMapper: ObjectMapper = JsonMapper.builder()
        .addModule(KotlinModule.Builder().build())
        .build()

    override fun serializer(): Serializer<T> = JsonSerializer(objectMapper)

    override fun deserializer(): Deserializer<T> = JsonDeserializer(objectMapper, clazz)

    private class JsonSerializer<T>(private val objectMapper: ObjectMapper) : Serializer<T> {
        override fun serialize(topic: String?, data: T?): ByteArray? {
            return data?.let { objectMapper.writeValueAsBytes(it) }
        }
    }

    private class JsonDeserializer<T>(
        private val objectMapper: ObjectMapper,
        private val clazz: Class<T>
    ) : Deserializer<T> {
        override fun deserialize(topic: String?, data: ByteArray?): T? {
            return data?.let { objectMapper.readValue(it, clazz) }
        }
    }

    companion object {
        inline fun <reified T> create(): JsonSerde<T> = JsonSerde(T::class.java)
    }
}