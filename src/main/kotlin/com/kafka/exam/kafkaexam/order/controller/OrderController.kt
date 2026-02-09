package com.kafka.exam.kafkaexam.order.controller

import com.kafka.exam.kafkaexam.order.controller.dto.CreateOrderRequest
import com.kafka.exam.kafkaexam.order.controller.dto.OrderResponse
import com.kafka.exam.kafkaexam.order.controller.dto.SagaStateResponse
import com.kafka.exam.kafkaexam.order.service.OrderService
import com.kafka.exam.kafkaexam.saga.core.SagaStateRepository
import com.kafka.exam.kafkaexam.saga.orchestrator.OrderSagaOrchestrator
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/orders")
class OrderController(
    private val orderSagaOrchestrator: OrderSagaOrchestrator,
    private val orderService: OrderService,
    private val sagaStateRepository: SagaStateRepository
) {

    @PostMapping
    fun createOrder(@RequestBody request: CreateOrderRequest): ResponseEntity<SagaStateResponse> {
        val sagaState = orderSagaOrchestrator.startOrderSaga(
            customerId = request.customerId,
            productId = request.productId,
            quantity = request.quantity,
            totalAmount = request.totalAmount
        )
        return ResponseEntity.ok(SagaStateResponse.from(sagaState))
    }

    @GetMapping("/{orderId}")
    fun getOrder(@PathVariable orderId: String): ResponseEntity<OrderResponse> {
        val order = orderService.findByOrderId(orderId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(OrderResponse.from(order))
    }

    @GetMapping("/saga/{sagaId}")
    fun getSagaState(@PathVariable sagaId: String): ResponseEntity<SagaStateResponse> {
        val sagaState = sagaStateRepository.findById(sagaId).orElse(null)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(SagaStateResponse.from(sagaState))
    }

    @GetMapping("/saga/{sagaId}/order")
    fun getOrderBySaga(@PathVariable sagaId: String): ResponseEntity<OrderResponse> {
        val order = orderService.findBySagaId(sagaId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(OrderResponse.from(order))
    }
}
