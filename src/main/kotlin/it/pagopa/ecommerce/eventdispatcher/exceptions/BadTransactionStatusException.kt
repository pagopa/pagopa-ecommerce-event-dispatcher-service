package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto

class BadTransactionStatusException(
  val transactionId: TransactionId,
  val expected: List<TransactionStatusDto>,
  val actual: TransactionStatusDto
) :
  RuntimeException(
    "Transaction with id ${transactionId.value()} was expected in status $expected but is in status $actual")
