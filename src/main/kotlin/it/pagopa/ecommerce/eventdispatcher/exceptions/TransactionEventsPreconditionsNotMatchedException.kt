package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode

class TransactionEventsPreconditionsNotMatchedException(
  transactionId: TransactionId,
  transactionEventCode: List<TransactionEventCode>
) :
  RuntimeException(
    "None of these events ${transactionEventCode.joinToString { "," }} found for transaction $transactionId")
