package it.pagopa.ecommerce.eventdispatcher.services.v1

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCompletedAuthorization
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.EuroUtils
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.queues.v1.getAuthorizationOutcome
import it.pagopa.ecommerce.eventdispatcher.queues.v1.reduceEvents
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

const val TIPO_VERSAMENTO_CP = "CP"

enum class TransactionDetailsStatusEnum(val status: String) {
  TRANSACTION_DETAILS_STATUS_CANCELED("Annullato"),
  TRANSACTION_DETAILS_STATUS_CONFIRMED("Confermato"),
  TRANSACTION_DETAILS_STATUS_DENIED("Rifiutato")
}

@Service("NodeServiceV1")
@Deprecated("Mark for deprecation in favor of V2 version")
class NodeService(
  @Autowired private val nodeClient: NodeClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) {
  val logger: Logger = LoggerFactory.getLogger(NodeService::class.java)

  suspend fun closePayment(
    transactionId: TransactionId,
    transactionOutcome: ClosePaymentOutcome
  ): ClosePaymentResponseDto {

    val baseTransaction =
      reduceEvents(
        Mono.just(transactionId.value()), transactionsEventStoreRepository, EmptyTransaction())

    val closePaymentRequest =
      baseTransaction.map {
        when (it.status) {
          TransactionStatusDto.CLOSURE_ERROR -> {
            if (it is TransactionWithClosureError) {
              val transactionAtPreviousState = it.transactionAtPreviousState()
              transactionAtPreviousState
                .map { trxPreviousStatus ->
                  when (transactionOutcome) {
                    ClosePaymentOutcome.KO -> {
                      trxPreviousStatus.fold(
                        { transactionWithCancellation ->
                          buildClosePaymentForCancellationRequest(
                            transactionWithCancellation, transactionId)
                        },
                        { transactionWithCompletedAuthorization ->
                          buildAuthorizationCompletedClosePaymentRequest(
                            transactionWithCompletedAuthorization,
                            transactionOutcome,
                            transactionId)
                        })
                    }
                    ClosePaymentOutcome.OK -> {
                      val authCompleted = trxPreviousStatus.get()
                      buildAuthorizationCompletedClosePaymentRequest(
                        authCompleted, transactionOutcome, transactionId)
                    }
                  }
                }
                .orElseThrow {
                  RuntimeException(
                    "Unexpected transactionAtPreviousStep: ${it.transactionAtPreviousState} ")
                }
            } else {
              throw RuntimeException(
                "Unexpected error while casting request into TransactionWithClosureError")
            }
          }
          TransactionStatusDto.CANCELLATION_REQUESTED ->
            buildClosePaymentForCancellationRequest(
              it as BaseTransactionWithCancellationRequested, transactionId)
          else -> {
            throw BadTransactionStatusException(
              transactionId = it.transactionId,
              expected =
                listOf(
                  TransactionStatusDto.CLOSURE_ERROR, TransactionStatusDto.CANCELLATION_REQUESTED),
              actual = it.status)
          }
        }
      }
    return nodeClient.closePayment(closePaymentRequest.awaitSingle()).awaitSingle()
  }

  private fun getTransactionDetailsStatus(it: BaseTransaction): String =
    when (getAuthorizationOutcome(it)) {
      AuthorizationResultDto.OK ->
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
      AuthorizationResultDto.KO ->
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
      else -> TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CANCELED.status
    }

  private fun getPaymentTypeCode(tx: BaseTransaction): String =
    when (tx) {
      is BaseTransactionWithRequestedAuthorization ->
        tx.transactionAuthorizationRequestData.paymentTypeCode
      is TransactionWithClosureError -> getPaymentTypeCode(tx.transactionAtPreviousState)
      else -> TIPO_VERSAMENTO_CP
    }

  private fun buildClosePaymentForCancellationRequest(
    transactionWithCancellation: BaseTransactionWithCancellationRequested,
    transactionId: TransactionId
  ): ClosePaymentRequestV2Dto {
    val amountEuroCents =
      BigDecimal(
        transactionWithCancellation.paymentNotices
          .stream()
          .mapToInt { el -> el.transactionAmount.value }
          .sum())
    return CardClosePaymentRequestV2Dto().apply {
      paymentTokens = transactionWithCancellation.paymentNotices.map { el -> el.paymentToken.value }
      outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.KO
      this.transactionId = transactionId.value()
      transactionDetails =
        TransactionDetailsDto().apply {
          transaction =
            TransactionDto().apply {
              this.transactionId = transactionId.value()
              this.transactionStatus = getTransactionDetailsStatus(transactionWithCancellation)
              this.creationDate = transactionWithCancellation.creationDate.toOffsetDateTime()
              this.amount = amountEuroCents
              this.grandTotal = amountEuroCents
            }
          info =
            InfoDto().apply {
              type = getPaymentTypeCode(transactionWithCancellation)
              clientId = transactionWithCancellation.clientId.name
            }
          user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
        }
    }
  }

  private fun buildAuthorizationCompletedClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId
  ): ClosePaymentRequestV2Dto {

    val fee = authCompleted.transactionAuthorizationRequestData.fee
    val amount = authCompleted.transactionAuthorizationRequestData.amount
    val totalAmount = amount.plus(fee)

    val feeEuroCents = BigDecimal(fee)
    val amountEuroCents = BigDecimal(amount)
    val totalAmountEuroCents = BigDecimal(totalAmount)

    val feeEuro = EuroUtils.euroCentsToEuro(fee)
    val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

    return CardClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
      if (transactionOutcome == ClosePaymentOutcome.OK) {
        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
        paymentMethod = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
        this.totalAmount = totalAmountEuro
        this.fee = feeEuro
        this.timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
      }
      this.transactionId = transactionId.value()
      additionalPaymentInformations =
        if (transactionOutcome == ClosePaymentOutcome.OK)
          CardAdditionalPaymentInformationsDto().apply {
            outcomePaymentGateway =
              CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.valueOf(
                authCompleted.transactionAuthorizationCompletedData.authorizationResultDto
                  .toString())
            this.authorizationCode =
              authCompleted.transactionAuthorizationCompletedData.authorizationCode
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                  authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(ZoneId.of("Europe/Paris"))
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            this.rrn = authCompleted.transactionAuthorizationCompletedData.rrn
            this.totalAmount = totalAmountEuro.toString()
          }
        else null
      transactionDetails =
        TransactionDetailsDto().apply {
          transaction =
            TransactionDto().apply {
              this.transactionId = transactionId.value()
              transactionStatus = getTransactionDetailsStatus(authCompleted)
              paymentGateway = authCompleted.transactionAuthorizationRequestData.paymentGateway.name
              this.fee = feeEuroCents
              this.amount = amountEuroCents
              this.grandTotal = totalAmountEuroCents
              rrn = authCompleted.transactionAuthorizationCompletedData.rrn
              errorCode =
                if (transactionOutcome == ClosePaymentOutcome.KO)
                  authCompleted.transactionAuthorizationCompletedData.errorCode
                else null
              authorizationCode =
                if (transactionOutcome == ClosePaymentOutcome.OK)
                  authCompleted.transactionAuthorizationCompletedData.authorizationCode
                else null
              creationDate = authCompleted.creationDate.toOffsetDateTime()
              timestampOperation =
                authCompleted.transactionAuthorizationCompletedData.timestampOperation
              psp =
                PspDto().apply {
                  idPsp = authCompleted.transactionAuthorizationRequestData.pspId
                  idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
                  businessName = authCompleted.transactionAuthorizationRequestData.pspBusinessName
                  brokerName = authCompleted.transactionAuthorizationRequestData.brokerName
                  pspOnUs = authCompleted.transactionAuthorizationRequestData.isPspOnUs
                }
            }
          info =
            InfoDto().apply {
              type = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
              brandLogo = authCompleted.transactionAuthorizationRequestData.logo.toString()
              brand = authCompleted.transactionAuthorizationRequestData.brand?.name
              paymentMethodName =
                authCompleted.transactionAuthorizationRequestData.paymentMethodName
              clientId = authCompleted.transactionActivatedData.clientId.name
            }
          user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
        }
    }
  }
}
