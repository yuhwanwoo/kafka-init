package com.kafka.exam.kafkaexam.schemaregistry

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SchemaRegistryService(
    private val schemaRegistryClient: SchemaRegistryClient
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun registerSchema(subject: String, schema: Schema): Int {
        return try {
            val avroSchema = AvroSchema(schema)
            val schemaId = schemaRegistryClient.register(subject, avroSchema)
            log.info("Schema registered: subject={}, schemaId={}", subject, schemaId)
            schemaId
        } catch (e: RestClientException) {
            log.error("Failed to register schema: subject={}, error={}", subject, e.message)
            throw SchemaRegistryException("Failed to register schema", e)
        }
    }

    fun getLatestSchema(subject: String): Schema? {
        return try {
            val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
            val schema = Schema.Parser().parse(schemaMetadata.schema)
            log.info("Retrieved latest schema: subject={}, version={}", subject, schemaMetadata.version)
            schema
        } catch (e: RestClientException) {
            log.warn("Schema not found: subject={}", subject)
            null
        }
    }

    fun getSchemaById(schemaId: Int): Schema? {
        return try {
            val parsedSchema = schemaRegistryClient.getSchemaById(schemaId)
            if (parsedSchema is AvroSchema) {
                parsedSchema.rawSchema() as Schema
            } else {
                null
            }
        } catch (e: RestClientException) {
            log.warn("Schema not found: schemaId={}", schemaId)
            null
        }
    }

    fun getSchemaByVersion(subject: String, version: Int): Schema? {
        return try {
            val schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version)
            Schema.Parser().parse(schemaMetadata.schema)
        } catch (e: RestClientException) {
            log.warn("Schema not found: subject={}, version={}", subject, version)
            null
        }
    }

    fun getAllVersions(subject: String): List<Int> {
        return try {
            schemaRegistryClient.getAllVersions(subject)
        } catch (e: RestClientException) {
            log.warn("No versions found: subject={}", subject)
            emptyList()
        }
    }

    fun getAllSubjects(): List<String> {
        return try {
            schemaRegistryClient.allSubjects.toList()
        } catch (e: RestClientException) {
            log.error("Failed to get subjects: {}", e.message)
            emptyList()
        }
    }

    fun checkCompatibility(subject: String, schema: Schema): Boolean {
        return try {
            val avroSchema = AvroSchema(schema)
            val compatible = schemaRegistryClient.testCompatibility(subject, avroSchema)
            log.info("Compatibility check: subject={}, compatible={}", subject, compatible)
            compatible
        } catch (e: RestClientException) {
            log.error("Compatibility check failed: subject={}, error={}", subject, e.message)
            false
        }
    }

    fun deleteSubject(subject: String): List<Int> {
        return try {
            val deletedVersions = schemaRegistryClient.deleteSubject(subject)
            log.info("Subject deleted: subject={}, versions={}", subject, deletedVersions)
            deletedVersions
        } catch (e: RestClientException) {
            log.error("Failed to delete subject: subject={}, error={}", subject, e.message)
            throw SchemaRegistryException("Failed to delete subject", e)
        }
    }

    fun deleteSchemaVersion(subject: String, version: Int): Int {
        return try {
            val deletedVersion = schemaRegistryClient.deleteSchemaVersion(subject, version.toString())
            log.info("Schema version deleted: subject={}, version={}", subject, deletedVersion)
            deletedVersion
        } catch (e: RestClientException) {
            log.error("Failed to delete schema version: subject={}, version={}, error={}", subject, version, e.message)
            throw SchemaRegistryException("Failed to delete schema version", e)
        }
    }

    fun getSubjectForTopic(topic: String, isKey: Boolean = false): String {
        return if (isKey) "$topic-key" else "$topic-value"
    }
}

class SchemaRegistryException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)