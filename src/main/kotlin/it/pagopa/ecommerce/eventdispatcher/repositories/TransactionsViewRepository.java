package it.pagopa.ecommerce.eventdispatcher.repositories;

import it.pagopa.ecommerce.commons.documents.BaseTransactionView;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface TransactionsViewRepository extends ReactiveCrudRepository<BaseTransactionView, String> {
    Mono<BaseTransactionView> findByTransactionId(String transactionId);
}
