package com.kafka.exam.kafkaexam.saga.core

enum class SagaStep(val order: Int) {
    CREATE_ORDER(1),
    PROCESS_PAYMENT(2),
    RESERVE_INVENTORY(3),
    COMPLETE_SAGA(4),

    // 보상 스텝들
    CANCEL_PAYMENT(10),
    CANCEL_ORDER(11),
    RELEASE_INVENTORY(12);

    fun nextStep(): SagaStep? = when (this) {
        CREATE_ORDER -> PROCESS_PAYMENT
        PROCESS_PAYMENT -> RESERVE_INVENTORY
        RESERVE_INVENTORY -> COMPLETE_SAGA
        COMPLETE_SAGA -> null
        CANCEL_PAYMENT -> CANCEL_ORDER
        CANCEL_ORDER -> null
        RELEASE_INVENTORY -> CANCEL_PAYMENT
    }

    fun compensationStep(): SagaStep? = when (this) {
        CREATE_ORDER -> null
        PROCESS_PAYMENT -> CANCEL_ORDER
        RESERVE_INVENTORY -> CANCEL_PAYMENT
        COMPLETE_SAGA -> null
        CANCEL_PAYMENT -> null
        CANCEL_ORDER -> null
        RELEASE_INVENTORY -> null
    }

    companion object {
        fun getCompensationChain(failedStep: SagaStep): List<SagaStep> {
            val chain = mutableListOf<SagaStep>()
            var current = failedStep.compensationStep()
            while (current != null) {
                chain.add(current)
                current = current.compensationStep()
            }
            return chain
        }
    }
}
