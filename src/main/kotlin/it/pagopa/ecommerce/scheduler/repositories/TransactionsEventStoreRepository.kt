package it.pagopa.ecommerce.scheduler.repositories

import it.pagopa.transactions.documents.TransactionEvent
import it.pagopa.transactions.utils.TransactionEventCode
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface TransactionsEventStoreRepository<T> : ReactiveCrudRepository<TransactionEvent<T>, String> {
    fun findByTransactionId(idTransaction: String): Flux<TransactionEvent<T>>
    fun findByTransactionIdAndEventCode(
        idTransaction: String,
        eventCode: TransactionEventCode
    ): Mono<TransactionEvent<T>>
}