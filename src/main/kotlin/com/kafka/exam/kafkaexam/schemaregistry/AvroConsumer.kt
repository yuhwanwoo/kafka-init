package com.kafka.exam.kafkaexam.schemaregistry

import com.kafka.exam.kafkaexam.avro.InventoryEvent
import com.kafka.exam.kafkaexam.avro.OrderEvent
import com.kafka.exam.kafkaexam.avro.PaymentEvent
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class AvroConsumer {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = ["\${kafka.avro.topic}"],
        containerFactory = "avroKafkaListenerContainerFactory"
    )
    fun consume(record: ConsumerRecord<String, SpecificRecord>, ack: Acknowledgment) {
        try {
            log.info(
                "Received Avro message: topic={}, partition={}, offset={}, schema={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.value().schema.name
            )

            when (val message = record.value()) {
                is OrderEvent -> handleOrderEvent(message)
                is PaymentEvent -> handlePaymentEvent(message)
                is InventoryEvent -> handleInventoryEvent(message)
                else -> log.warn("Unknown Avro message type: {}", message.schema.name)
            }

            ack.acknowledge()
            log.debug("Message acknowledged: offset={}", record.offset())

        } catch (e: Exception) {
            log.error(
                "Error processing Avro message: topic={}, offset={}, error={}",
                record.topic(),
                record.offset(),
                e.message,
                e
            )
            throw e
        }
    }

    private fun handleOrderEvent(event: OrderEvent) {
        log.info(
            "Processing OrderEvent: eventId={}, orderId={}, type={}, amount={}",
            event.eventId,
            event.orderId,
            event.eventType,
            event.totalAmount
        )
    }

    private fun handlePaymentEvent(event: PaymentEvent) {
        log.info(
            "Processing PaymentEvent: eventId={}, paymentId={}, type={}, amount={}",
            event.eventId,
            event.paymentId,
            event.eventType,
            event.amount
        )
    }

    private fun handleInventoryEvent(event: InventoryEvent) {
        log.info(
            "Processing InventoryEvent: eventId={}, productId={}, type={}, quantity={}",
            event.eventId,
            event.productId,
            event.eventType,
            event.quantity
        )
    }
}