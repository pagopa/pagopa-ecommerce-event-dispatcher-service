package it.pagopa.transactions.documents;

import it.pagopa.generated.transactions.server.model.TransactionStatusDto;
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
public class TransactionRefundedData {

    private TransactionStatusDto statusBeforeRefunded;
}
