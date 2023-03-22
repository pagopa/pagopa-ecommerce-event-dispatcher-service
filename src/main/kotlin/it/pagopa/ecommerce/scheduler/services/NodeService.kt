package it.pagopa.ecommerce.scheduler.services

import io.vavr.Tuple
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.scheduler.client.NodeClient
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventsInconsistentException
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventsPreconditionsNotMatchedException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.OffsetDateTime
import java.util.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class NodeService(
  @Autowired private val nodeClient: NodeClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) {
  var logger: Logger = LoggerFactory.getLogger(NodeService::class.java)
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

    // Retrieved to check if the user canceled event request exists
    val userCanceledEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT)
        .cast(TransactionUserCanceledEvent::class.java)
        .awaitSingleOrNull()

    val authEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT)
        .cast(TransactionAuthorizationRequestedEvent::class.java)
        .awaitSingleOrNull()

    val closePaymentRequest =
      mono { userCanceledEvent }
        .map {
          ClosePaymentRequestV2Dto().apply {
            paymentTokens = activatedEvent.data.paymentNotices.map { it.paymentToken }
            outcome = transactionOutcome
            this.transactionId = transactionId.toString()
          }
        }
        .filter(Objects::nonNull)
        .switchIfEmpty(
          mono { authEvent }
            .map { ev ->
              ClosePaymentRequestV2Dto().apply {
                paymentTokens = activatedEvent.data.paymentNotices.map { it.paymentToken }
                outcome = transactionOutcome
                idPSP = ev.data.pspId
                paymentMethod = ev.data.paymentTypeCode
                idBrokerPSP = ev.data.brokerName
                idChannel = ev.data.pspChannelCode
                this.transactionId = transactionId.toString()
                totalAmount = (ev.data.amount.plus(ev.data.fee)).toBigDecimal()
                fee = ev.data.fee.toBigDecimal()
                timestampOperation = OffsetDateTime.now()
                additionalPaymentInformations = mapOf()
              }
            }
            .filter(Objects::nonNull)
            .switchIfEmpty(
              Mono.error {
                TransactionEventsPreconditionsNotMatchedException(
                  transactionId,
                  listOf(
                    TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT,
                    TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
              }))

    val checkAndGetClosePaymentRequest =
      mono { Tuple.of(userCanceledEvent, authEvent) }
        .filter { t -> t._1() == null || t._2() == null }
        .flatMap { closePaymentRequest }
        .switchIfEmpty(
          Mono.error(
            TransactionEventsInconsistentException(
              transactionId,
              listOf(
                TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT,
                TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))))

    return nodeClient.closePayment(checkAndGetClosePaymentRequest.awaitSingle()).awaitSingle()
  }
}
