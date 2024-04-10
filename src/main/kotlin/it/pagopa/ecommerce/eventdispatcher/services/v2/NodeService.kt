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
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
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
    transactionOutcome: ClosePaymentOutcome
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
                  ClosePaymentOutcome.KO -> {
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
                  ClosePaymentOutcome.OK -> {
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
    // In case of cancellation we only populate the fields shared by both
    // `CardClosePaymentRequestV2Dto` and `RedirectClosePaymentRequestV2Dto`,
    // so either implementation is fine.
    // TODO: Fix upstream openapi-generator to better handle `allOf` and `oneOf` composition
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

    val closePaymentTransactionDetails =
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
              if (transactionOutcome == ClosePaymentOutcome.KO)
                getAuthorizationErrorCode(
                  authCompleted.transactionAuthorizationCompletedData
                    .transactionGatewayAuthorizationData)
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
                  authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                else -> null
              }
            paymentMethodName = authCompleted.transactionAuthorizationRequestData.paymentMethodName
            clientId = authCompleted.transactionActivatedData.clientId.name
          }
        user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
      }

    return when (authRequestedData.type!!) {
      // TODO: Add here support for APM methods and wallet
      TransactionGatewayAuthorizationRequestedData.AuthorizationDataType.PGS ->
        buildAuthorizationCompletedCardClosePaymentRequest(
          authCompleted,
          transactionOutcome,
          transactionId,
          totalAmountEuro,
          feeEuro,
          closePaymentTransactionDetails)
      TransactionGatewayAuthorizationRequestedData.AuthorizationDataType.NPG ->
        when (authCompleted.transactionAuthorizationRequestData.paymentTypeCode) {
          "CP" ->
            buildAuthorizationCompletedCardClosePaymentRequest(
              authCompleted,
              transactionOutcome,
              transactionId,
              totalAmountEuro,
              feeEuro,
              closePaymentTransactionDetails)
          "PPAL" ->
            buildAuthorizationCompletedPayPalClosePaymentRequest(
              authCompleted,
              transactionOutcome,
              transactionId,
              totalAmountEuro,
              feeEuro,
              closePaymentTransactionDetails)
          "BPAY" ->
            buildAuthorizationCompletedBancomatPayClosePaymentRequest(
              authCompleted,
              transactionOutcome,
              transactionId,
              totalAmountEuro,
              feeEuro,
              closePaymentTransactionDetails)
          "MYBK" ->
            buildAuthorizationCompletedMyBankClosePaymentRequest(
              authCompleted,
              transactionOutcome,
              transactionId,
              totalAmountEuro,
              feeEuro,
              closePaymentTransactionDetails)
          else ->
            throw IllegalArgumentException(
              "Unhandled or invalid payment type code: '%s'".format(
                authCompleted.transactionAuthorizationRequestData.paymentTypeCode))
        }
      TransactionGatewayAuthorizationRequestedData.AuthorizationDataType.REDIRECT ->
        buildAuthorizationCompletedRedirectClosePaymentRequest(
          authCompleted,
          transactionOutcome,
          transactionId,
          totalAmountEuro,
          feeEuro,
          closePaymentTransactionDetails)
    }
  }

  private fun buildAuthorizationCompletedRedirectClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ) =
    RedirectClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = RedirectClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
      this.transactionId = transactionId.value()

      if (transactionOutcome == ClosePaymentOutcome.OK) {
        this.totalAmount = totalAmountEuro
        this.fee = feeEuro
        this.timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
        paymentMethod = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
      }

      additionalPaymentInformations =
        if (transactionOutcome == ClosePaymentOutcome.OK) {
          RedirectAdditionalPaymentInformationsDto().apply {
            idTransaction = transactionId.value()
            idPSPTransaction =
              authCompleted.transactionAuthorizationRequestData.authorizationRequestId
            this.totalAmount = totalAmountEuro.toString()
            this.authorizationCode =
              authCompleted.transactionAuthorizationCompletedData.authorizationCode
            this.fee = feeEuro.toString()
            timestampOperation =
              OffsetDateTime.parse(
                authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          }
        } else null
      transactionDetails = closePaymentTransactionDetails
    }

  private fun buildAuthorizationCompletedCardClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ) =
    CardClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
      this.transactionId = transactionId.value()

      if (transactionOutcome == ClosePaymentOutcome.OK) {
        timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation)
        this.totalAmount = totalAmountEuro
        this.fee = feeEuro
        this.timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
        paymentMethod = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
      }

      additionalPaymentInformations =
        if (transactionOutcome == ClosePaymentOutcome.OK)
          CardAdditionalPaymentInformationsDto().apply {
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
                .toString()
            this.rrn = authCompleted.transactionAuthorizationCompletedData.rrn
            this.totalAmount = totalAmountEuro.toString()
          }
        else null
      transactionDetails = closePaymentTransactionDetails
    }

  private fun buildAuthorizationCompletedPayPalClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ) =
    PayPalClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = PayPalClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
      this.transactionId = transactionId.value()

      if (transactionOutcome == ClosePaymentOutcome.OK) {
        timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation)
        this.totalAmount = totalAmountEuro
        this.fee = feeEuro
        this.timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
        paymentMethod = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
      }

      additionalPaymentInformations =
        if (transactionOutcome == ClosePaymentOutcome.OK) {
          val npgTransactionGatewayAuthorizationData =
            authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
              as NpgTransactionGatewayAuthorizationData

          PayPalAdditionalPaymentInformationsDto().apply {
            this.transactionId = npgTransactionGatewayAuthorizationData.operationId
            this.pspTransactionId = npgTransactionGatewayAuthorizationData.paymentEndToEndId
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            this.totalAmount = totalAmountEuro.toString()
          }
        } else null
      transactionDetails = closePaymentTransactionDetails
    }

  private fun buildAuthorizationCompletedBancomatPayClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ) =
    BancomatPayClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = BancomatPayClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
      this.transactionId = transactionId.value()

      if (transactionOutcome == ClosePaymentOutcome.OK) {
        timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation)
        this.totalAmount = totalAmountEuro
        this.fee = feeEuro
        this.timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
        paymentMethod = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
      }

      additionalPaymentInformations =
        if (transactionOutcome == ClosePaymentOutcome.OK) {
          val npgTransactionGatewayAuthorizationData =
            authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
              as NpgTransactionGatewayAuthorizationData
          BancomatPayAdditionalPaymentInformationsDto().apply {
            this.transactionId = npgTransactionGatewayAuthorizationData.operationId
            this.outcomePaymentGateway =
              BancomatPayAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.valueOf(
                transactionOutcome.name)
            this.totalAmount = totalAmountEuro.toString()
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            this.authorizationCode = npgTransactionGatewayAuthorizationData.operationId
            // this field is not to be set actually, here set explicitly to null as reminder
            this.seteMail(null)
          }
        } else null
      transactionDetails = closePaymentTransactionDetails
    }

  private fun buildAuthorizationCompletedMyBankClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ) =
    MyBankClosePaymentRequestV2Dto().apply {
      paymentTokens =
        authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
      outcome = MyBankClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
      this.transactionId = transactionId.value()

      if (transactionOutcome == ClosePaymentOutcome.OK) {
        timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation)
        this.totalAmount = totalAmountEuro
        this.fee = feeEuro
        this.timestampOperation =
          OffsetDateTime.parse(
            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
        paymentMethod = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
      }

      additionalPaymentInformations =
        if (transactionOutcome == ClosePaymentOutcome.OK) {
          val npgTransactionGatewayAuthorizationData =
            authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
              as NpgTransactionGatewayAuthorizationData

          MyBankAdditionalPaymentInformationsDto().apply {
            this.transactionId = npgTransactionGatewayAuthorizationData.operationId
            this.mybankTransactionId = npgTransactionGatewayAuthorizationData.paymentEndToEndId
            this.totalAmount = totalAmountEuro.toString()
            this.fee = feeEuro.toString()
            this.validationServiceId = npgTransactionGatewayAuthorizationData.validationServiceId
            this.timestampOperation =
              OffsetDateTime.parse(
                authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            // this field is not to be set actually, here set explicitly to null as reminder
            this.seteMail(null)
          }
        } else null
      transactionDetails = closePaymentTransactionDetails
    }

  private fun getOutcomePaymentGateway(
    transactionGatewayAuthData: TransactionGatewayAuthorizationData
  ): CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum =
    when (transactionGatewayAuthData) {
      is PgsTransactionGatewayAuthorizationData ->
        if (transactionGatewayAuthData.authorizationResultDto == AuthorizationResultDto.OK) {
          CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
        } else {
          CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.KO
        }
      is NpgTransactionGatewayAuthorizationData ->
        if (transactionGatewayAuthData.operationResult == OperationResultDto.EXECUTED) {
          CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
        } else {
          CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.KO
        }
      is RedirectTransactionGatewayAuthorizationData ->
        if (transactionGatewayAuthData.outcome ==
          RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
          CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
        } else {
          CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.KO
        }
    }

  private fun getAuthorizationErrorCode(
    transactionGatewayAuthData: TransactionGatewayAuthorizationData
  ): String? =
    when (transactionGatewayAuthData) {
      is PgsTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
      is NpgTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
      is RedirectTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
    }
}
