package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.commons.documents.v1.TransactionActivatedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.scheduler.client.NodeClient
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.scheduler.queues.TransactionExpirationQueueConsumer
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.OffsetDateTime
import java.util.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class NodeService(
  @Autowired private val nodeClient: NodeClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionExpirationQueueConsumer::class.java)
  suspend fun closePayment(
    transactionId: UUID,
    transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum
  ): ClosePaymentResponseDto {
    val transactionActivatedEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT

    val activatedEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(transactionId.toString(), transactionActivatedEventCode)
        .cast(TransactionActivatedEvent::class.java)
        .awaitSingleOrNull()
        ?: throw TransactionEventNotFoundException(transactionId, transactionActivatedEventCode)

    val transactionAuthRequestedEventCode =
      TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT

    val authEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(
          transactionId.toString(), transactionAuthRequestedEventCode)
        .cast(TransactionAuthorizationRequestedEvent::class.java)
        .awaitSingleOrNull()

    logger.info("Invoking closePayment with outcome {}", transactionOutcome)

    val closePaymentRequest =
      ClosePaymentRequestV2Dto().apply {
        paymentTokens = activatedEvent.data.paymentNotices.map { it.paymentToken }
        outcome = transactionOutcome
        idPSP = authEvent?.data?.pspId
        paymentMethod = authEvent?.data?.paymentTypeCode
        idBrokerPSP = authEvent?.data?.brokerName
        idChannel = authEvent?.data?.pspChannelCode
        this.transactionId = transactionId.toString()
        totalAmount = (authEvent?.data?.amount?.plus(authEvent.data.fee))?.toBigDecimal()
        fee = authEvent?.data?.fee?.toBigDecimal()
        timestampOperation = OffsetDateTime.now()
        additionalPaymentInformations = mapOf()
      }
    return nodeClient.closePayment(closePaymentRequest).awaitSingle()
  }
}
