package it.pagopa.ecommerce.scheduler.repositories

import it.pagopa.transactions.documents.TransactionEvent
import it.pagopa.transactions.utils.TransactionEventCode
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Mono

interface TransactionsEventStoreRepository<T> : ReactiveCrudRepository<TransactionEvent<T>, String> {
    fun findByTransactionIdAndEventCode(idTransaction: String, transactionEventCode: TransactionEventCode): Mono<TransactionEvent<T>>
}