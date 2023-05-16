package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId

class TooLateRetryAttemptException(transactionId: TransactionId, eventCode: TransactionEventCode) :
  NoRetryAttemptsLeftException(transactionId, eventCode)
