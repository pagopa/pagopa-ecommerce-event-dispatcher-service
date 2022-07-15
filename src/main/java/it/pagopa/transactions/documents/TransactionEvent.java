package it.pagopa.transactions.documents;

import com.azure.spring.data.cosmos.core.mapping.PartitionKey;
import it.pagopa.transactions.utils.TransactionEventCode;
import lombok.Data;
import lombok.Generated;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.UUID;

import static java.time.ZonedDateTime.now;

@Data
@Document(collection = "eventstore")
@Generated
public abstract sealed class TransactionEvent<T>
        permits
        TransactionInitEvent,
        TransactionAuthorizationRequestedEvent,
        TransactionAuthorizationStatusUpdatedEvent,
        TransactionClosureSentEvent {

    @Id
    private String id;
    @PartitionKey
    private String transactionId;
    private String rptId;
    private String paymentToken;
    private TransactionEventCode eventCode;
    private String creationDate;
    private T data;

    TransactionEvent(String transactionId, String rptId, String paymentToken, TransactionEventCode eventCode, T data) {
        this.id = UUID.randomUUID().toString();
        this.transactionId = transactionId;
        this.rptId = rptId;
        this.eventCode = eventCode;
        this.paymentToken = paymentToken;
        this.data = data;
        this.creationDate = now().toString();
    }
}

