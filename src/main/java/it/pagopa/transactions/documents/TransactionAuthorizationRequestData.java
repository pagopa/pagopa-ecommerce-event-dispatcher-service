package it.pagopa.transactions.documents;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

@AllArgsConstructor
@Data
@Document
public class TransactionAuthorizationRequestData {
    private int amount;
    private int fee;
    private String paymentInstrumentId;
    private String pspId;
    private String paymentTypeCode;
    private String brokerName;
    private String pspChannelCode;
    private String paymentMethodName;
    private String pspBusinessName;
    private String authorizationRequestId;
}
