package com.kafka.exam.kafkaexam.inventory.controller

import com.kafka.exam.kafkaexam.inventory.domain.Inventory
import com.kafka.exam.kafkaexam.inventory.service.InventoryService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/inventory")
class InventoryController(
    private val inventoryService: InventoryService
) {

    @PostMapping("/{productId}")
    fun initializeInventory(
        @PathVariable productId: String,
        @RequestParam quantity: Int
    ): ResponseEntity<InventoryResponse> {
        val inventory = inventoryService.initializeInventory(productId, quantity)
        return ResponseEntity.ok(InventoryResponse.from(inventory))
    }

    @GetMapping("/{productId}")
    fun getInventory(@PathVariable productId: String): ResponseEntity<InventoryResponse> {
        val inventory = inventoryService.findByProductId(productId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(InventoryResponse.from(inventory))
    }
}

data class InventoryResponse(
    val productId: String,
    val quantity: Int,
    val reservedQuantity: Int,
    val availableQuantity: Int
) {
    companion object {
        fun from(inventory: Inventory) = InventoryResponse(
            productId = inventory.productId,
            quantity = inventory.quantity,
            reservedQuantity = inventory.reservedQuantity,
            availableQuantity = inventory.availableQuantity()
        )
    }
}
