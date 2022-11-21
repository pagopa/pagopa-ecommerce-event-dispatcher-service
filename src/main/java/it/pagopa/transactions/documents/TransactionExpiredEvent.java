package it.pagopa.transactions.documents;

import it.pagopa.transactions.utils.TransactionEventCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "eventstore")
@NoArgsConstructor
@ToString(callSuper = true)
public final class TransactionExpiredEvent extends TransactionEvent<TransactionExpiredData> {
    public TransactionExpiredEvent(String transactionId, String rptId, String paymentToken, TransactionExpiredData data) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_EXPIRED_EVENT, data);
    }
}
