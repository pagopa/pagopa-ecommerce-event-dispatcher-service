package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode

class TransactionEventNotFoundException(
  transactionId: TransactionId,
  transactionEventCode: TransactionEventCode
) : RuntimeException("Event $transactionEventCode not found for transaction $transactionId")
