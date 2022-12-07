package it.pagopa.ecommerce.scheduler.repositories;

import it.pagopa.ecommerce.commons.documents.Transaction;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface TransactionsViewRepository extends ReactiveCrudRepository<Transaction, String> {
    Mono<Transaction> findByPaymentToken(String paymentToken);
    Mono<Transaction> findByTransactionId(String transactionId);
}
