package it.pagopa.transactions.documents;

import it.pagopa.transactions.utils.TransactionEventCode;

public final class TransactionClosureErrorEvent extends TransactionEvent<Void> {
    public TransactionClosureErrorEvent(String transactionId, String rptId, String paymentToken) {
        super(transactionId, rptId, paymentToken, TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT, null);
    }
}
