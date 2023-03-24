package it.pagopa.ecommerce.scheduler.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import java.util.*

class TransactionEventsPreconditionsNotMatchedException(
  transactionId: UUID,
  transactionEventCode: List<TransactionEventCode>
) :
  RuntimeException(
    "None of these events ${transactionEventCode.joinToString { "," }} found for transaction $transactionId")
