package com.kafka.exam.kafkaexam.inventory.domain

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface InventoryRepository : JpaRepository<Inventory, String>
