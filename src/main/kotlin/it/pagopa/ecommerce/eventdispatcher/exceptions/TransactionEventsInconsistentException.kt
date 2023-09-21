package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.TransactionId

class TransactionEventsInconsistentException(
  transactionId: TransactionId,
  transactionEventCode: List<TransactionEventCode>
) :
  RuntimeException(
    "Events ${transactionEventCode.joinToString { "," }} found together for transaction $transactionId")
