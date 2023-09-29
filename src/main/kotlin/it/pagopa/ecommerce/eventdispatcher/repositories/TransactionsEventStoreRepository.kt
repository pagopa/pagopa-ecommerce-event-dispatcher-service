package it.pagopa.ecommerce.eventdispatcher.repositories

import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux

interface TransactionsEventStoreRepository<T> :
  ReactiveCrudRepository<BaseTransactionEvent<T>, String> {
  fun findByTransactionIdOrderByCreationDateAsc(
    idTransaction: String
  ): Flux<BaseTransactionEvent<T>>
}
