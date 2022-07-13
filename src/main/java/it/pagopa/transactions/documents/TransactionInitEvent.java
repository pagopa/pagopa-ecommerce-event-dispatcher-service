package it.pagopa.transactions.documents;

import it.pagopa.transactions.utils.TransactionEventCode;
import lombok.Generated;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "eventstore")
@Generated
public final class TransactionInitEvent extends TransactionEvent<TransactionInitData> {
    public TransactionInitEvent(String transactionId, String rptId, String paymentToken, TransactionInitData data) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_INITIALIZED_EVENT, data);
    }
}
