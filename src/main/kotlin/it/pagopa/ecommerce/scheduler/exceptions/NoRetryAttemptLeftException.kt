package it.pagopa.ecommerce.scheduler.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId

class NoRetryAttemptLeftException(transactionId: TransactionId, eventCode: TransactionEventCode) :
  RuntimeException(
    "No attempts left for retry event $eventCode for transaction id: ${transactionId.value}")
