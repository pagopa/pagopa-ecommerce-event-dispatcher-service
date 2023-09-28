package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId

open class NoRetryAttemptsLeftException(message: String) : RuntimeException(message) {
  constructor(
    transactionId: TransactionId,
    eventCode: String
  ) : this(
    "No attempts left for retry event $eventCode for transaction id: ${transactionId.value()}")
}
