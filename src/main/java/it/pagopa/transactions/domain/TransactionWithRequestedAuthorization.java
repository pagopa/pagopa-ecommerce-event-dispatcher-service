package it.pagopa.transactions.domain;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;
import it.pagopa.transactions.documents.TransactionAuthorizationRequestedEvent;
import it.pagopa.transactions.documents.TransactionAuthorizationStatusUpdatedEvent;
import it.pagopa.transactions.documents.TransactionEvent;
import it.pagopa.transactions.domain.pojos.BaseTransactionWithPaymentToken;
import it.pagopa.transactions.domain.pojos.BaseTransactionWithRequestedAuthorization;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public final class TransactionWithRequestedAuthorization extends BaseTransactionWithRequestedAuthorization implements EventUpdatable<TransactionWithCompletedAuthorization, TransactionAuthorizationStatusUpdatedEvent>, Transaction {
    TransactionWithRequestedAuthorization(BaseTransactionWithPaymentToken transaction, TransactionAuthorizationRequestedEvent event) {
        super(transaction, event.getData());
    }

    @Override
    public TransactionWithCompletedAuthorization apply(TransactionAuthorizationStatusUpdatedEvent event) {
        return new TransactionWithCompletedAuthorization(this.withStatus(event.getData().getNewTransactionStatus()), event);
    }

    @Override
    public Transaction applyEvent(TransactionEvent<?> event) {
        if (event instanceof TransactionAuthorizationStatusUpdatedEvent authorizationStatusUpdatedEvent) {
            return this.apply(authorizationStatusUpdatedEvent);
        } else {
            return this;
        }
    }

    @Override
    public TransactionWithRequestedAuthorization withStatus(TransactionStatusDto status) {
        return new TransactionWithRequestedAuthorization(
                new TransactionActivated(
                        this.getTransactionId(),
                        new PaymentToken(this.getTransactionActivatedData().getPaymentToken()),
                        this.getRptId(),
                        this.getDescription(),
                        this.getAmount(),
                        this.getEmail(),
                        this.getTransactionActivatedData().getFaultCode(),
                        this.getTransactionActivatedData().getFaultCodeString(),
                        this.getCreationDate(),
                        status
                ),
                new TransactionAuthorizationRequestedEvent(
                        this.getTransactionId().value().toString(),
                        this.getRptId().value(),
                        this.getTransactionActivatedData().getPaymentToken(),
                        this.getTransactionAuthorizationRequestData()
                )
        );
    }
}
