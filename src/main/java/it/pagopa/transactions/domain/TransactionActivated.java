package it.pagopa.transactions.domain;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;
import it.pagopa.transactions.documents.TransactionActivatedData;
import it.pagopa.transactions.documents.TransactionActivatedEvent;
import it.pagopa.transactions.documents.TransactionAuthorizationRequestedEvent;
import it.pagopa.transactions.documents.TransactionEvent;
import it.pagopa.transactions.domain.pojos.BaseTransactionWithPaymentToken;
import lombok.EqualsAndHashCode;

import java.time.ZonedDateTime;

@EqualsAndHashCode(callSuper = true)
public final class TransactionActivated extends BaseTransactionWithPaymentToken implements EventUpdatable<TransactionWithRequestedAuthorization, TransactionAuthorizationRequestedEvent>, Transaction {
    public TransactionActivated(TransactionId transactionId, PaymentToken paymentToken, RptId rptId, TransactionDescription description, TransactionAmount amount, Email email, String faultCode, String faultCodeString, ZonedDateTime creationDate, TransactionStatusDto status) {
        super(new TransactionActivationRequested(transactionId, rptId, description, amount, email, creationDate, status), new TransactionActivatedData(description.value(), amount.value(), email.value(), faultCode, faultCodeString, paymentToken.value()));
    }

    public TransactionActivated(TransactionId transactionId, PaymentToken paymentToken, RptId rptId, TransactionDescription description, TransactionAmount amount, Email email, String faultCode, String faultCodeString, TransactionStatusDto status) {
        this(transactionId, paymentToken, rptId, description, amount, email, faultCode, faultCodeString, ZonedDateTime.now(), status);
    }

    public TransactionActivated(TransactionActivationRequested transactionActivationRequested, TransactionActivatedEvent event) {
        super(transactionActivationRequested, event.getData());
    }

    @Override
    public TransactionWithRequestedAuthorization apply(TransactionAuthorizationRequestedEvent event) {
        return new TransactionWithRequestedAuthorization(this, event);
    }

    @Override
    public Transaction applyEvent(TransactionEvent<?> event) {
        if (event instanceof TransactionAuthorizationRequestedEvent) {
            return this.apply((TransactionAuthorizationRequestedEvent) event);
        } else {
            return this;
        }
    }

    @Override
    public TransactionActivated withStatus(TransactionStatusDto status) {
        return new TransactionActivated(
                this.getTransactionId(),
                new PaymentToken(this.getTransactionActivatedData().getPaymentToken()),
                this.getRptId(),
                this.getDescription(),
                this.getAmount(),
                this.getEmail(),
                this.getTransactionActivatedData().getFaultCode(),
                this.getTransactionActivatedData().getFaultCodeString(),
                this.getCreationDate(),
                this.getStatus()
        );
    }
}
