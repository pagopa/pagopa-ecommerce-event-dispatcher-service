package it.pagopa.transactions.domain.pojos;

import it.pagopa.transactions.documents.TransactionAuthorizationStatusUpdateData;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@ToString
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public abstract class BaseTransactionWithCompletedAuthorization extends BaseTransactionWithRequestedAuthorization {
    TransactionAuthorizationStatusUpdateData transactionAuthorizationStatusUpdateData;

    protected BaseTransactionWithCompletedAuthorization(BaseTransactionWithRequestedAuthorization baseTransaction, TransactionAuthorizationStatusUpdateData transactionAuthorizationStatusUpdateData) {
        super(baseTransaction, baseTransaction.getTransactionAuthorizationRequestData());
        this.transactionAuthorizationStatusUpdateData = transactionAuthorizationStatusUpdateData;
    }
}
