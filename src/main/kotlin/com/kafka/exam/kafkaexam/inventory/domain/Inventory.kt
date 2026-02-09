package com.kafka.exam.kafkaexam.inventory.domain

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "inventory")
class Inventory(
    @Id
    val productId: String,

    @Column(nullable = false)
    var quantity: Int,

    @Column(nullable = false)
    var reservedQuantity: Int = 0,

    @Column(nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun availableQuantity(): Int = quantity - reservedQuantity

    fun reserve(amount: Int): Boolean {
        if (availableQuantity() < amount) {
            return false
        }
        reservedQuantity += amount
        updatedAt = LocalDateTime.now()
        return true
    }

    fun release(amount: Int) {
        reservedQuantity = maxOf(0, reservedQuantity - amount)
        updatedAt = LocalDateTime.now()
    }

    fun confirmReservation(amount: Int) {
        quantity -= amount
        reservedQuantity -= amount
        updatedAt = LocalDateTime.now()
    }
}
