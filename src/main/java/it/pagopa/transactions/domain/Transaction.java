package it.pagopa.transactions.domain;

import it.pagopa.transactions.annotations.AggregateRoot;
import it.pagopa.transactions.documents.TransactionEvent;

@AggregateRoot
public sealed interface Transaction permits
        EmptyTransaction,
        TransactionActivated,
        TransactionActivationRequested,
        TransactionWithRequestedAuthorization,
        TransactionWithCompletedAuthorization,
        TransactionClosed
{
    Transaction applyEvent(TransactionEvent<?> event);
}
