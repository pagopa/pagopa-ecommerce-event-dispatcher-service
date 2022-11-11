package it.pagopa.transactions.documents;

import it.pagopa.transactions.utils.TransactionEventCode;
import lombok.Generated;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "eventstore")
@Generated
@NoArgsConstructor
@ToString(callSuper = true)
public final class TransactionActivatedEvent extends TransactionEvent<TransactionActivatedData> {
    public TransactionActivatedEvent(String transactionId, String rptId, String paymentToken, String creationDate, TransactionActivatedData data) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_ACTIVATED_EVENT, creationDate, data);
    }

    public TransactionActivatedEvent(String transactionId, String rptId, String paymentToken, TransactionActivatedData data) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_ACTIVATED_EVENT, data);
    }
}
