package it.pagopa.transactions.domain.pojos;

import it.pagopa.transactions.documents.TransactionActivatedData;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@ToString
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public abstract class BaseTransactionWithPaymentToken extends BaseTransaction {
    TransactionActivatedData transactionActivatedData;

    protected BaseTransactionWithPaymentToken(
            BaseTransaction baseTransaction,
            TransactionActivatedData transactionActivatedData) {
        super(
                baseTransaction.getTransactionId(),
                baseTransaction.getRptId(),
                baseTransaction.getDescription(),
                baseTransaction.getAmount(),
                baseTransaction.getEmail(),
                baseTransaction.getCreationDate(),
                baseTransaction.getStatus());

        this.transactionActivatedData = transactionActivatedData;
    }
}
