package it.pagopa.ecommerce.scheduler.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId

class NoRetryAttemptsLeftException(
  val transactionId: TransactionId,
  val eventCode: TransactionEventCode
) :
  RuntimeException(
    "No attempts left for retry event $eventCode for transaction id: ${transactionId.value}")
