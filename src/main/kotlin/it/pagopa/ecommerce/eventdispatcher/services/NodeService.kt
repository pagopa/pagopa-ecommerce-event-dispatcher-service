package it.pagopa.ecommerce.eventdispatcher.services

import io.vavr.Tuple
import it.pagopa.ecommerce.commons.documents.v1.TransactionActivatedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationCompletedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserCanceledEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventsInconsistentException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventsPreconditionsNotMatchedException
import it.pagopa.ecommerce.eventdispatcher.queues.wasAuthorizationRequested
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

const val TIPO_VERSAMENTO_CP = "CP"

@Service
class NodeService(
  @Autowired private val nodeClient: NodeClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) {
  var logger: Logger = LoggerFactory.getLogger(NodeService::class.java)

  suspend fun closePayment(
    transactionId: TransactionId,
    transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum
  ): ClosePaymentResponseDto {
    val transactionActivatedEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
    val activatedEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(transactionId.value(), transactionActivatedEventCode)
        .cast(TransactionActivatedEvent::class.java)
        .awaitSingleOrNull()
        ?: throw TransactionEventNotFoundException(transactionId, transactionActivatedEventCode)

    // Retrieved to check if the user canceled event request exists
    val userCanceledEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(
          transactionId.value(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT)
        .cast(TransactionUserCanceledEvent::class.java)
        .awaitSingleOrNull()

    val authEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(
          transactionId.value(), TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT)
        .cast(TransactionAuthorizationRequestedEvent::class.java)
        .awaitSingleOrNull()

    val authCompletedEvent =
      transactionsEventStoreRepository
        .findByTransactionIdAndEventCode(
          transactionId.value(),
          TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT)
        .cast(TransactionAuthorizationCompletedEvent::class.java)
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
              when (transactionOutcome) {
                ClosePaymentRequestV2Dto.OutcomeEnum.KO ->
                  ClosePaymentRequestV2Dto().apply {
                    paymentTokens = activatedEvent.data.paymentNotices.map { it.paymentToken }
                    outcome = transactionOutcome
                    this.transactionId = transactionId.toString()
                    transactionDetails =
                      TransactionDetailsDto().apply {
                        transaction = TransactionDto().apply {
                          //transactionId = ev.transactionId
                          transactionStatus = "Confermato"
                          fee = ev.data.fee.toBigDecimal()
                          amount = ev.data.amount.toBigDecimal()
                          grandTotal = (ev.data.fee + ev.data.amount).toBigDecimal()
                          rrn = authCompletedEvent!!.data.rrn
                          authorizationCode = authCompletedEvent.data.authorizationCode
                          creationDate = OffsetDateTime.parse(activatedEvent.creationDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                          psp = PspDto().apply {
                            idPsp = ev.data.pspId
                            idChannel = ev.data.pspChannelCode
                            businessName = ev.data.pspBusinessName
                          }
                        }.transactionId(ev.transactionId)
                        info = InfoDto().apply {
                          type = ev.data.paymentTypeCode
                          brandLogo = ev.data.logo.path
                        }
                        user = UserDto().apply {
                          type = UserDto.TypeEnum.GUEST
                        }
                      }
                  }
                ClosePaymentRequestV2Dto.OutcomeEnum.OK -> {
                  if(authEvent != null && authCompletedEvent == null && userCanceledEvent == null) {
                    throw TransactionEventNotFoundException(transactionId, TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT)
                  }
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
                    this.timestampOperation =
                      OffsetDateTime.parse(
                        authCompletedEvent!!.data.timestampOperation,
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    additionalPaymentInformations =
                      AdditionalPaymentInformationsDto().apply {
                        tipoVersamento = TIPO_VERSAMENTO_CP
                        outcomePaymentGateway =
                          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.valueOf(
                            authCompletedEvent.data.authorizationResultDto.toString())
                        this.authorizationCode = authCompletedEvent.data.authorizationCode
                        fee = ev.data.fee.toBigDecimal()
                        this.timestampOperation =
                          OffsetDateTime.parse(
                            authCompletedEvent.data.timestampOperation,
                            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        rrn = authCompletedEvent.data.rrn
                      }
                    transactionDetails =
                      TransactionDetailsDto().apply {
                        transaction = TransactionDto().apply {
                          //transactionId = ev.transactionId
                          transactionStatus = "Confermato"
                          fee = ev.data.fee.toBigDecimal()
                          amount = ev.data.amount.toBigDecimal()
                          grandTotal = (ev.data.fee + ev.data.amount).toBigDecimal()
                          rrn = authCompletedEvent.data.rrn
                          authorizationCode = authCompletedEvent.data.authorizationCode
                          creationDate = OffsetDateTime.parse(activatedEvent.creationDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                          psp = PspDto().apply {
                            idPsp = ev.data.pspId
                            idChannel = ev.data.pspChannelCode
                            businessName = ev.data.pspBusinessName
                          }
                        }.transactionId(ev.transactionId)
                        info = InfoDto().apply {
                          type = ev.data.paymentTypeCode
                          brandLogo = ev.data.logo.path
                        }
                        user = UserDto().apply {
                          type = UserDto.TypeEnum.GUEST
                        }
                      }
                  }
                }
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
