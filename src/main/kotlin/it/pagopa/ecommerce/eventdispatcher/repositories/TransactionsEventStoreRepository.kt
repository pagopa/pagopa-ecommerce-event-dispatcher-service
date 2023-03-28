package it.pagopa.ecommerce.eventdispatcher.repositories

import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface TransactionsEventStoreRepository<T> :
  ReactiveCrudRepository<TransactionEvent<T>, String> {
  fun findByTransactionId(idTransaction: String): Flux<TransactionEvent<T>>
  fun findByTransactionIdAndEventCode(
    idTransaction: String,
    eventCode: TransactionEventCode
  ): Mono<TransactionEvent<T>>
}
