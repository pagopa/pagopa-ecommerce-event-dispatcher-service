package it.pagopa.ecommerce.scheduler.repositories;

import it.pagopa.ecommerce.commons.documents.v1.Transaction;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface TransactionsViewRepository extends ReactiveCrudRepository<Transaction, String> {
    Mono<Transaction> findByTransactionId(String transactionId);
}
