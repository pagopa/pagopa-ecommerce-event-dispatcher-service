package it.pagopa.transactions.documents;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document
@AllArgsConstructor
@NoArgsConstructor
@Generated
public class TransactionActivationRequestedData {
    //TODO Add other fields?
    private String description;
    private Integer amount;
    private String email;
    private String faultCode; // TODO enum with all PAA & PTT
    private String faultCodeString;
    private String paymentContextCode;
}
