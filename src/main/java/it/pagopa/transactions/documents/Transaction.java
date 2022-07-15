package it.pagopa.transactions.documents;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;

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
    private TransactionStatusDto status;
    private String creationDate;

    public Transaction(String transactionId, String paymentToken, String rptId, String description, int amount, TransactionStatusDto status) {
        this(transactionId, paymentToken, rptId, description, amount, status, ZonedDateTime.now().toString());
    }

    public Transaction(String transactionId, String paymentToken, String rptId, String description, int amount, TransactionStatusDto status,
            ZonedDateTime creationDate) {
        this(transactionId, paymentToken, rptId, description, amount, status, creationDate.toString());
    }

    @PersistenceConstructor
    public Transaction(String transactionId, String paymentToken, String rptId, String description, int amount, TransactionStatusDto status,
            String creationDate) {
        this.transactionId = transactionId;
        this.rptId = rptId;
        this.description = description;
        this.paymentToken = paymentToken;
        this.amount = amount;
        this.status = status;
        this.creationDate = creationDate;
    }
}
