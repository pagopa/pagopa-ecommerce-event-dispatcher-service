package it.pagopa.transactions.documents;

import lombok.Generated;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Document
@AllArgsConstructor
@NoArgsConstructor
@Generated
public class TransactionActivatedData {

    private String description;
    private Integer amount;
    private String email;
    private String faultCode; // TODO enum with all PAA & PTT
    private String faultCodeString;
    private String paymentToken;
}
