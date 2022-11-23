package it.pagopa.transactions.documents;

import it.pagopa.transactions.utils.TransactionEventCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "eventstore")
@NoArgsConstructor
@ToString(callSuper = true)
public final class TransactionRefundedEvent extends TransactionEvent<TransactionRefundedData> {
    public TransactionRefundedEvent(String transactionId, String rptId, String paymentToken, TransactionRefundedData data) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_REFUNDED_EVENT, data);
    }
}
