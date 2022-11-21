package it.pagopa.transactions.domain;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;
import it.pagopa.transactions.documents.TransactionAuthorizationRequestedEvent;
import it.pagopa.transactions.documents.TransactionAuthorizationStatusUpdatedEvent;
import it.pagopa.transactions.documents.TransactionClosureSentEvent;
import it.pagopa.transactions.documents.TransactionEvent;
import it.pagopa.transactions.domain.pojos.BaseTransactionWithCompletedAuthorization;
import it.pagopa.transactions.domain.pojos.BaseTransactionWithRequestedAuthorization;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public final class TransactionWithCompletedAuthorization extends BaseTransactionWithCompletedAuthorization implements EventUpdatable<TransactionClosed, TransactionClosureSentEvent>, Transaction {
    public TransactionWithCompletedAuthorization(BaseTransactionWithRequestedAuthorization baseTransaction, TransactionAuthorizationStatusUpdatedEvent event) {
        super(baseTransaction, event.getData());
    }

    @Override
    public TransactionClosed apply(TransactionClosureSentEvent event) {
        return new TransactionClosed(this.withStatus(event.getData().getNewTransactionStatus()), event);
    }

    @Override
    public Transaction applyEvent(TransactionEvent<?> event) {
        if (event instanceof TransactionClosureSentEvent closureSentEvent) {
            return this.apply(closureSentEvent);
        } else {
            return this;
        }
    }

    @Override
    public TransactionWithCompletedAuthorization withStatus(TransactionStatusDto status) {
        return new TransactionWithCompletedAuthorization(
                new TransactionWithRequestedAuthorization(
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
                ),
                new TransactionAuthorizationStatusUpdatedEvent(
                        this.getTransactionId().value().toString(),
                        this.getRptId().value(),
                        this.getTransactionActivatedData().getPaymentToken(),
                        this.getTransactionAuthorizationStatusUpdateData()
                )
        );
    }
}
