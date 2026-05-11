package it.pagopa.ecommerce.eventdispatcher.repositories

import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import reactor.core.publisher.Flux

interface TransactionsEventStoreRepository<T> :
  ReactiveMongoRepository<BaseTransactionEvent<T>, String> {
  fun findByTransactionIdOrderByCreationDateAsc(
    idTransaction: String
  ): Flux<BaseTransactionEvent<T>>
}
