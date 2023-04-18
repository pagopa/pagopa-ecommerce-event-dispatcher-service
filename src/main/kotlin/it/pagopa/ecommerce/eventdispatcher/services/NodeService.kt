package it.pagopa.ecommerce.eventdispatcher.services

import io.vavr.Tuple
import it.pagopa.ecommerce.commons.documents.v1.TransactionActivatedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserCanceledEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventsInconsistentException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventsPreconditionsNotMatchedException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
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
    transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum,
    authorizationCode: Optional<String>
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
              val timestamp =
                OffsetDateTime
                  .now() // FIXME this timestamp should the one coming from authorizationResultDto
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
                timestampOperation = timestamp
                additionalPaymentInformations =
                  mapOf(
                    "outcome_payment_gateway" to transactionOutcome.value,
                    "authorization_code" to authorizationCode.orElseGet { "" },
                    "tipoVersamento" to "CP",
                    "rrn" to "123456789",
                    "fee" to ev.data.fee.toBigDecimal().toString(),
                    // bug CHK-1410: date formatted to yyyy-MM-ddTHH:mm:ss truncating millis
                    "timestampOperation" to
                      timestamp
                        .toLocalDateTime()
                        .truncatedTo(ChronoUnit.SECONDS)
                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    "totalAmount" to (ev.data.amount.plus(ev.data.fee)).toBigDecimal().toString())
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
