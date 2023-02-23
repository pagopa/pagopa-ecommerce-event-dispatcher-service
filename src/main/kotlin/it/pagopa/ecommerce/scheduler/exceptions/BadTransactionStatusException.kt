package it.pagopa.ecommerce.scheduler.exceptions

import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto

class BadTransactionStatusException(val transactionId: TransactionId, val expected: TransactionStatusDto, val actual: TransactionStatusDto) : RuntimeException("Transaction with id ${transactionId.value} was expected in status $expected but is in status $actual")
