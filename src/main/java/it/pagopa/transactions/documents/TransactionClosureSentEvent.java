package it.pagopa.transactions.documents;

import it.pagopa.transactions.utils.TransactionEventCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "eventstore")
@NoArgsConstructor
@ToString(callSuper = true)
public final class TransactionClosureSentEvent extends TransactionEvent<TransactionClosureSendData> {
    public TransactionClosureSentEvent(String transactionId, String rptId, String paymentToken, TransactionClosureSendData data) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_CLOSURE_SENT_EVENT, data);
    }
}
