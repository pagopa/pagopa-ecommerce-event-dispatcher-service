package it.pagopa.transactions.domain;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;
import it.pagopa.transactions.documents.TransactionActivatedEvent;
import it.pagopa.transactions.documents.TransactionEvent;
import it.pagopa.transactions.domain.pojos.BaseTransaction;
import lombok.EqualsAndHashCode;

import java.time.ZonedDateTime;

import static java.time.ZonedDateTime.now;

@EqualsAndHashCode(callSuper = true)
public final class TransactionActivationRequested extends BaseTransaction implements EventUpdatable<TransactionActivated, TransactionActivatedEvent>, Transaction {
    public TransactionActivationRequested(TransactionId transactionId, RptId rptId, TransactionDescription description, TransactionAmount amount, Email email, ZonedDateTime creationDate, TransactionStatusDto status) {
        super(transactionId, rptId, description, amount, email, creationDate, status);
    }

    public TransactionActivationRequested(TransactionId transactionId, RptId rptId, TransactionDescription description, TransactionAmount amount, Email email, TransactionStatusDto status) {
        super(transactionId, rptId, description, amount, email, now(), status);
    }

    @Override
    public TransactionActivated apply(TransactionActivatedEvent event) {
        return new TransactionActivated(this.withStatus(TransactionStatusDto.ACTIVATED), event);
    }

    @Override
    public Transaction applyEvent(TransactionEvent<?> event) {
        if (event instanceof TransactionActivatedEvent transactionActivatedEvent) {
            return this.apply(transactionActivatedEvent);
        } else {
            return this;
        }
    }

    @Override
    public TransactionActivationRequested withStatus(TransactionStatusDto status) {
        return new TransactionActivationRequested(
                this.getTransactionId(),
                this.getRptId(),
                this.getDescription(),
                this.getAmount(),
                this.getEmail(),
                this.getCreationDate(),
                status
        );
    }
}
