package it.pagopa.transactions.documents;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;
import it.pagopa.transactions.domain.TransactionActivated;
import it.pagopa.transactions.domain.TransactionActivationRequested;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.ZonedDateTime;

@Data
@Document(collection = "view")
public class Transaction {
    @Id
    private String transactionId;
    private String paymentToken;
    private String rptId;
    private String description;
    private int amount;
    private String email;
    private TransactionStatusDto status;
    private String creationDate;

    public Transaction(String transactionId, String paymentToken, String rptId, String description, int amount, String email, TransactionStatusDto status) {
        this(transactionId, paymentToken, rptId, description, amount, email, status, ZonedDateTime.now().toString());
    }

    public Transaction(String transactionId, String paymentToken, String rptId, String description, int amount, String email, TransactionStatusDto status,
                       ZonedDateTime creationDate) {
        this(transactionId, paymentToken, rptId, description, amount, email, status, creationDate.toString());
    }

    @PersistenceConstructor
    public Transaction(String transactionId, String paymentToken, String rptId, String description, int amount, String email, TransactionStatusDto status,
                       String creationDate) {
        this.transactionId = transactionId;
        this.rptId = rptId;
        this.description = description;
        this.paymentToken = paymentToken;
        this.amount = amount;
        this.email = email;
        this.status = status;
        this.creationDate = creationDate;
    }

    public static Transaction from(TransactionActivated transaction) {
        return new Transaction(
                transaction.getTransactionId().value().toString(),
                transaction.getTransactionActivatedData().getPaymentToken(),
                transaction.getRptId().value(),
                transaction.getDescription().value(),
                transaction.getAmount().value(),
                transaction.getEmail().value(),
                transaction.getStatus(),
                transaction.getCreationDate().toString()
        );
    }

    public static Transaction from(TransactionActivationRequested transaction) {
        return new Transaction(
                transaction.getTransactionId().value().toString(),
                null,
                transaction.getRptId().value(),
                transaction.getDescription().value(),
                transaction.getAmount().value(),
                transaction.getEmail().value(),
                transaction.getStatus(),
                transaction.getCreationDate().toString());
    }
}
