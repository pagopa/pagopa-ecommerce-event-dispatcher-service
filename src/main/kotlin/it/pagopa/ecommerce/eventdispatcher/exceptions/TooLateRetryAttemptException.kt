package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import java.time.Instant

class TooLateRetryAttemptException(
  transactionId: TransactionId,
  eventCode: TransactionEventCode,
  visibilityTimeout: Instant
) :
  NoRetryAttemptsLeftException(
    "No time left for retry event $eventCode for transaction id: ${transactionId.value()}. " +
      "Retry time: $visibilityTimeout will be too late")
