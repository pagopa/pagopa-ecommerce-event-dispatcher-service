package it.pagopa.ecommerce.scheduler.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import java.util.*

class TransactionEventsInconsistentException(
  transactionId: UUID,
  transactionEventCode: List<TransactionEventCode>
) :
  RuntimeException(
    "Events ${transactionEventCode.joinToString { "," }} found together for transaction $transactionId")
