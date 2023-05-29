package it.pagopa.ecommerce.eventdispatcher.repositories

import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux

interface TransactionsEventStoreRepository<T> :
  ReactiveCrudRepository<TransactionEvent<T>, String> {
  fun findByTransactionIdOrderByCreationDateAsc(idTransaction: String): Flux<TransactionEvent<T>>
}
