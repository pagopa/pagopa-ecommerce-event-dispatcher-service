package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import java.time.Instant

class TooLateRetryAttemptException(
  transactionId: TransactionId,
  eventCode: String,
  visibilityTimeout: Instant
) :
  NoRetryAttemptsLeftException(
    "No time left for retry event $eventCode for transaction id: ${transactionId.value()}. " +
      "Retry time: $visibilityTimeout will be too late")
