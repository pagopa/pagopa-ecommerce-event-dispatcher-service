package it.pagopa.ecommerce.eventdispatcher.repositories;

import it.pagopa.ecommerce.commons.documents.BaseTransactionView;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface TransactionsViewRepository extends ReactiveMongoRepository<BaseTransactionView, String> {
    Mono<BaseTransactionView> findByTransactionId(String transactionId);
}
