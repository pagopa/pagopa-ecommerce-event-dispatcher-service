package it.pagopa.ecommerce.eventdispatcher.services.v2

import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.authorization.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureRequested
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.EuroUtils
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.getAuthorizationOutcome
import it.pagopa.ecommerce.eventdispatcher.queues.v2.reduceEvents
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*
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

@Service("NodeServiceV2")
class NodeService(
  @Autowired private val nodeClient: NodeClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) {
  var logger: Logger = LoggerFactory.getLogger(NodeService::class.java)

  suspend fun closePayment(
    transactionId: TransactionId,
    transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum
  ): ClosePaymentResponseDto {

    val baseTransaction =
      reduceEvents(
        Mono.just(transactionId.value()), transactionsEventStoreRepository, EmptyTransaction())
    val closePaymentRequest =
      baseTransaction.map {
        when (it.status) {
          TransactionStatusDto.CLOSURE_REQUESTED,
          TransactionStatusDto.CLOSURE_ERROR -> {
            val transactionAtPreviousState =
              when (it) {
                is TransactionWithClosureError -> it.transactionAtPreviousState()
                is TransactionWithClosureRequested ->
                  Optional.of(Either.right(it as BaseTransactionWithClosureRequested))
                else ->
                  /*
                   * We should never go into this else branch because it will mean that transaction status is not
                   * coherent with reduced domain object!
                   */
                  throw RuntimeException(
                    "Mismatching between transaction status and reduced transaction object detected!")
              }
            transactionAtPreviousState
              .map { trxPreviousStatus ->
                when (transactionOutcome) {
                  ClosePaymentRequestV2Dto.OutcomeEnum.KO -> {
                    trxPreviousStatus.fold(
                      { transactionWithCancellation ->
                        buildClosePaymentForCancellationRequest(
                          transactionWithCancellation, transactionId)
                      },
                      { transactionWithCompletedAuthorization ->
                        buildAuthorizationCompletedClosePaymentRequest(
                          transactionWithCompletedAuthorization, transactionOutcome, transactionId)
                      })
                  }
                  ClosePaymentRequestV2Dto.OutcomeEnum.OK -> {
                    val authCompleted = trxPreviousStatus.get()
                    buildAuthorizationCompletedClosePaymentRequest(
                      authCompleted, transactionOutcome, transactionId)
                  }
                }
              }
              .orElseThrow {
                RuntimeException(
                  "Unexpected transactionAtPreviousStep: $transactionAtPreviousState")
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
    return ClosePaymentRequestV2Dto().apply {
      paymentTokens = transactionWithCancellation.paymentNotices.map { el -> el.paymentToken.value }
      outcome = ClosePaymentRequestV2Dto.OutcomeEnum.KO
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
    transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum,
    transactionId: TransactionId
  ): ClosePaymentRequestV2Dto {
    val authRequestedData =
      authCompleted.transactionAuthorizationRequestData.transactionGatewayAuthorizationRequestedData

    val fee = authCompleted.transactionAuthorizationRequestData.fee
    val amount = authCompleted.transactionAuthorizationRequestData.amount
    val totalAmount = amount.plus(fee)

    val feeEuroCents = BigDecimal(fee)
    val amountEuroCents = BigDecimal(amount)
    val totalAmountEuroCents = BigDecimal(totalAmount)

    val feeEuro = EuroUtils.euroCentsToEuro(fee)
    val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

    return ClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = transactionOutcome
      if (transactionOutcome == ClosePaymentRequestV2Dto.OutcomeEnum.OK) {
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
        if (transactionOutcome == ClosePaymentRequestV2Dto.OutcomeEnum.OK)
          AdditionalPaymentInformationsDto().apply {
            outcomePaymentGateway =
              getOutcomePaymentGateway(
                authCompleted.transactionAuthorizationCompletedData
                  .transactionGatewayAuthorizationData)
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
              grandTotal = totalAmountEuroCents
              rrn = authCompleted.transactionAuthorizationCompletedData.rrn
              errorCode =
                if (transactionOutcome == ClosePaymentRequestV2Dto.OutcomeEnum.KO)
                  getAuthorizationErrorCode(
                    authCompleted.transactionAuthorizationCompletedData
                      .transactionGatewayAuthorizationData)
                else null
              authorizationCode =
                if (transactionOutcome == ClosePaymentRequestV2Dto.OutcomeEnum.OK)
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
              brandLogo =
                authCompleted.transactionAuthorizationRequestData
                  .transactionGatewayAuthorizationRequestedData
                  .logo
                  .toString()
              brand =
                when (authRequestedData) {
                  is NpgTransactionGatewayAuthorizationRequestedData -> authRequestedData.brand
                  is PgsTransactionGatewayAuthorizationRequestedData ->
                    authRequestedData.brand?.toString()
                  is RedirectTransactionGatewayAuthorizationRequestedData ->
                    authRequestedData.paymentMethodType.toString()
                  else -> null
                }
              paymentMethodName =
                authCompleted.transactionAuthorizationRequestData.paymentMethodName
              clientId = authCompleted.transactionActivatedData.clientId.name
            }
          user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
        }
    }
  }

  private fun getOutcomePaymentGateway(
    transactionGatewayAuthData: TransactionGatewayAuthorizationData
  ): AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum =
    when (transactionGatewayAuthData) {
      is PgsTransactionGatewayAuthorizationData ->
        if (transactionGatewayAuthData.authorizationResultDto == AuthorizationResultDto.OK) {
          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
        } else {
          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.KO
        }
      is NpgTransactionGatewayAuthorizationData ->
        if (transactionGatewayAuthData.operationResult == OperationResultDto.EXECUTED) {
          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
        } else {
          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.KO
        }
      is RedirectTransactionGatewayAuthorizationData ->
        if (transactionGatewayAuthData.outcome ==
          RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
        } else {
          AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.KO
        }
    }

  private fun getAuthorizationErrorCode(
    transactionGatewayAuthData: TransactionGatewayAuthorizationData
  ): String? =
    when (transactionGatewayAuthData) {
      is PgsTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
      // TODO handle error code into Close Payment for NPG flow
      is NpgTransactionGatewayAuthorizationData -> null
      is RedirectTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
    }
}
