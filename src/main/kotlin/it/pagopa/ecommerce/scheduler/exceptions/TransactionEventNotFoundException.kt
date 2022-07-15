package it.pagopa.ecommerce.scheduler.exceptions

import it.pagopa.transactions.utils.TransactionEventCode
import java.util.*

class TransactionEventNotFoundException(transactionId: UUID, transactionEventCode: TransactionEventCode) : RuntimeException("Event $transactionEventCode not found for transaction $transactionId")