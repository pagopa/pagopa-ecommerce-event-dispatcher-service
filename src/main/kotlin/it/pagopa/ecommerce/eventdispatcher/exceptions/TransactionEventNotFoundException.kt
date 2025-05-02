package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionId

class TransactionEventNotFoundException(
  transactionId: TransactionId,
  transactionEventCode: TransactionEventCode
) : RuntimeException("Event $transactionEventCode not found for transaction $transactionId")
