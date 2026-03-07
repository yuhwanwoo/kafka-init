package com.kafka.exam.kafkaexam.schemaregistry

import com.kafka.exam.kafkaexam.avro.InventoryEvent
import com.kafka.exam.kafkaexam.avro.InventoryEventType
import com.kafka.exam.kafkaexam.avro.OrderEvent
import com.kafka.exam.kafkaexam.avro.OrderEventType
import com.kafka.exam.kafkaexam.avro.PaymentEvent
import com.kafka.exam.kafkaexam.avro.PaymentEventType
import com.kafka.exam.kafkaexam.avro.PaymentMethod
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant
import java.util.UUID

@RestController
@RequestMapping("/api/schema-registry")
class SchemaRegistryController(
    private val schemaRegistryService: SchemaRegistryService,
    private val avroProducer: AvroProducer
) {
    @Value("\${kafka.avro.topic}")
    private lateinit var avroTopic: String

    @GetMapping("/subjects")
    fun getAllSubjects(): ResponseEntity<List<String>> {
        return ResponseEntity.ok(schemaRegistryService.getAllSubjects())
    }

    @GetMapping("/subjects/{subject}/versions")
    fun getVersions(@PathVariable subject: String): ResponseEntity<List<Int>> {
        return ResponseEntity.ok(schemaRegistryService.getAllVersions(subject))
    }

    @GetMapping("/subjects/{subject}/versions/latest")
    fun getLatestSchema(@PathVariable subject: String): ResponseEntity<SchemaInfo> {
        val schema = schemaRegistryService.getLatestSchema(subject)
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(
            SchemaInfo(
                subject = subject,
                schemaType = "AVRO",
                schema = schema.toString(true)
            )
        )
    }

    @GetMapping("/subjects/{subject}/versions/{version}")
    fun getSchemaByVersion(
        @PathVariable subject: String,
        @PathVariable version: Int
    ): ResponseEntity<SchemaInfo> {
        val schema = schemaRegistryService.getSchemaByVersion(subject, version)
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(
            SchemaInfo(
                subject = subject,
                version = version,
                schemaType = "AVRO",
                schema = schema.toString(true)
            )
        )
    }

    @GetMapping("/schemas/ids/{id}")
    fun getSchemaById(@PathVariable id: Int): ResponseEntity<String> {
        val schema = schemaRegistryService.getSchemaById(id)
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(schema.toString(true))
    }

    @DeleteMapping("/subjects/{subject}")
    fun deleteSubject(@PathVariable subject: String): ResponseEntity<List<Int>> {
        val deletedVersions = schemaRegistryService.deleteSubject(subject)
        return ResponseEntity.ok(deletedVersions)
    }

    @PostMapping("/test/order")
    fun sendTestOrderEvent(@RequestBody request: TestOrderRequest): ResponseEntity<SendResult> {
        val event = OrderEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setOrderId(request.orderId ?: UUID.randomUUID().toString())
            .setCustomerId(request.customerId ?: "CUST-001")
            .setProductId(request.productId ?: "PROD-001")
            .setQuantity(request.quantity ?: 1)
            .setTotalAmount(request.totalAmount ?: 10000L)
            .setEventType(OrderEventType.valueOf(request.eventType ?: "CREATED"))
            .setTimestamp(Instant.now().toEpochMilli())
            .setMetadata(request.metadata)
            .build()

        val future = avroProducer.send(avroTopic, event.orderId.toString(), event)
        val result = future.get()

        return ResponseEntity.ok(
            SendResult(
                topic = result.recordMetadata.topic(),
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                schemaId = extractSchemaId(event),
                message = "Order event sent successfully"
            )
        )
    }

    @PostMapping("/test/payment")
    fun sendTestPaymentEvent(@RequestBody request: TestPaymentRequest): ResponseEntity<SendResult> {
        val event = PaymentEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setPaymentId(request.paymentId ?: UUID.randomUUID().toString())
            .setOrderId(request.orderId ?: UUID.randomUUID().toString())
            .setAmount(request.amount ?: 10000L)
            .setCurrency(request.currency ?: "KRW")
            .setPaymentMethod(PaymentMethod.valueOf(request.paymentMethod ?: "CARD"))
            .setEventType(PaymentEventType.valueOf(request.eventType ?: "COMPLETED"))
            .setTimestamp(Instant.now().toEpochMilli())
            .setFailureReason(request.failureReason)
            .build()

        val future = avroProducer.send(avroTopic, event.orderId.toString(), event)
        val result = future.get()

        return ResponseEntity.ok(
            SendResult(
                topic = result.recordMetadata.topic(),
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                schemaId = extractSchemaId(event),
                message = "Payment event sent successfully"
            )
        )
    }

    @PostMapping("/test/inventory")
    fun sendTestInventoryEvent(@RequestBody request: TestInventoryRequest): ResponseEntity<SendResult> {
        val event = InventoryEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setProductId(request.productId ?: "PROD-001")
            .setOrderId(request.orderId)
            .setQuantity(request.quantity ?: 1)
            .setAvailableStock(request.availableStock ?: 100)
            .setEventType(InventoryEventType.valueOf(request.eventType ?: "RESERVED"))
            .setTimestamp(Instant.now().toEpochMilli())
            .build()

        val future = avroProducer.send(avroTopic, event.productId.toString(), event)
        val result = future.get()

        return ResponseEntity.ok(
            SendResult(
                topic = result.recordMetadata.topic(),
                partition = result.recordMetadata.partition(),
                offset = result.recordMetadata.offset(),
                schemaId = extractSchemaId(event),
                message = "Inventory event sent successfully"
            )
        )
    }

    @GetMapping("/compatibility/{subject}")
    fun checkCompatibility(
        @PathVariable subject: String,
        @RequestParam schemaType: String
    ): ResponseEntity<CompatibilityResult> {
        val schema = when (schemaType.uppercase()) {
            "ORDER" -> OrderEvent.getClassSchema()
            "PAYMENT" -> PaymentEvent.getClassSchema()
            "INVENTORY" -> InventoryEvent.getClassSchema()
            else -> return ResponseEntity.badRequest().build()
        }

        val compatible = schemaRegistryService.checkCompatibility(subject, schema)
        return ResponseEntity.ok(
            CompatibilityResult(
                subject = subject,
                schemaType = schemaType,
                isCompatible = compatible
            )
        )
    }

    private fun extractSchemaId(event: Any): Int? {
        return null
    }
}

data class SchemaInfo(
    val subject: String,
    val version: Int? = null,
    val schemaType: String,
    val schema: String
)

data class SendResult(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val schemaId: Int?,
    val message: String
)

data class CompatibilityResult(
    val subject: String,
    val schemaType: String,
    val isCompatible: Boolean
)

data class TestOrderRequest(
    val orderId: String? = null,
    val customerId: String? = null,
    val productId: String? = null,
    val quantity: Int? = null,
    val totalAmount: Long? = null,
    val eventType: String? = null,
    val metadata: Map<String, String>? = null
)

data class TestPaymentRequest(
    val paymentId: String? = null,
    val orderId: String? = null,
    val amount: Long? = null,
    val currency: String? = null,
    val paymentMethod: String? = null,
    val eventType: String? = null,
    val failureReason: String? = null
)

data class TestInventoryRequest(
    val productId: String? = null,
    val orderId: String? = null,
    val quantity: Int? = null,
    val availableStock: Int? = null,
    val eventType: String? = null
)