package it.pagopa.ecommerce.eventdispatcher.services.v2

import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.authorization.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.WalletInfo.CardWalletDetails
import it.pagopa.ecommerce.commons.documents.v2.authorization.WalletInfo.PaypalWalletDetails
import it.pagopa.ecommerce.commons.domain.Email
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
import it.pagopa.ecommerce.eventdispatcher.utils.ConfidentialDataUtils
import it.pagopa.ecommerce.eventdispatcher.utils.PaymentCode
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
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
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired private val confidentialDataUtils: ConfidentialDataUtils
) {
  var logger: Logger = LoggerFactory.getLogger(NodeService::class.java)

  private val closePaymentZoneId = ZoneId.of("Europe/Paris")

  suspend fun closePayment(
    transactionId: TransactionId,
    transactionOutcome: ClosePaymentOutcome
  ): ClosePaymentResponseDto {

    val baseTransaction =
      reduceEvents(
        Mono.just(transactionId.value()), transactionsEventStoreRepository, EmptyTransaction())
    val closePaymentRequest =
      baseTransaction
        .cast(BaseTransactionWithPaymentToken::class.java)
        .flatMap { trx ->
          buildUserInfo(trx, transactionOutcome).map { userInfo -> trx to userInfo }
        }
        .flatMap { (baseTransaction, userInfo) ->
          when (baseTransaction.status) {
            TransactionStatusDto.CLOSURE_REQUESTED,
            TransactionStatusDto.CLOSURE_ERROR -> {
              val transactionAtPreviousState =
                when (baseTransaction) {
                  is TransactionWithClosureError -> baseTransaction.transactionAtPreviousState()
                  is TransactionWithClosureRequested ->
                    Optional.of(
                      Either.right(baseTransaction as BaseTransactionWithClosureRequested))
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
                          Mono.just(
                            buildClosePaymentForCancellationRequest(
                              transactionWithCancellation = transactionWithCancellation,
                              transactionId = transactionId,
                              userDto = userInfo))
                        },
                        { transactionWithCompletedAuthorization ->
                          buildAuthorizationCompletedClosePaymentRequest(
                            authCompleted = transactionWithCompletedAuthorization,
                            transactionOutcome = transactionOutcome,
                            transactionId = transactionId,
                            userDto = userInfo)
                        })
                    }
                    ClosePaymentOutcome.OK -> {
                      val authCompleted = trxPreviousStatus.get()
                      buildAuthorizationCompletedClosePaymentRequest(
                        authCompleted = authCompleted,
                        transactionOutcome = transactionOutcome,
                        transactionId = transactionId,
                        userDto = userInfo)
                    }
                  }
                }
                .map { it.map { c -> c } }
                .orElseThrow {
                  RuntimeException(
                    "Unexpected transactionAtPreviousStep: $transactionAtPreviousState")
                }
            }
            TransactionStatusDto.CANCELLATION_REQUESTED ->
              Mono.just(
                buildClosePaymentForCancellationRequest(
                  transactionWithCancellation =
                    baseTransaction as BaseTransactionWithCancellationRequested,
                  transactionId = transactionId,
                  userDto = userInfo))
            else -> {
              throw BadTransactionStatusException(
                transactionId = baseTransaction.transactionId,
                expected =
                  listOf(
                    TransactionStatusDto.CLOSURE_ERROR,
                    TransactionStatusDto.CANCELLATION_REQUESTED),
                actual = baseTransaction.status)
            }
          }
        }
        .map { it }
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
    transactionId: TransactionId,
    userDto: UserDto
  ): ClosePaymentRequestV2Dto {
    val amountEuroCents =
      BigDecimal(
        transactionWithCancellation.paymentNotices
          .stream()
          .mapToInt { el -> el.transactionAmount.value }
          .sum())
    logger.info(
      "Building close payment for cancellation transactionId: [{}], effective clientId: [{}], activation client: [{}]",
      transactionId.value(),
      transactionWithCancellation.clientId.effectiveClient.name,
      transactionWithCancellation.clientId.name,
    )
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
              clientId = transactionWithCancellation.clientId.effectiveClient.name
            }
          user = userDto
        }
    }
  }

  private fun buildAuthorizationCompletedClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    userDto: UserDto
  ): Mono<ClosePaymentRequestV2Dto> {
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

    val walletInfo =
      if (authRequestedData is NpgTransactionGatewayAuthorizationRequestedData) {
        authRequestedData.walletInfo
      } else {
        null
      }
    val walletDetails = walletInfo?.walletDetails
    val (walletBin, walletLastFourDigits, walletMaskedEmail) =
      when (walletDetails) {
        is CardWalletDetails -> Triple(walletDetails.bin, walletDetails.lastFourDigits, null)
        is PaypalWalletDetails -> Triple(null, null, walletDetails.maskedEmail)
        else -> Triple(null, null, null)
      }

    logger.info(
      "Building close payment for completed transactionId: [{}], effective clientId: [{}], activation client: [{}]",
      transactionId.value(),
      authCompleted.transactionActivatedData.clientId.effectiveClient.name,
      authCompleted.transactionActivatedData.clientId.name)
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
                is NpgTransactionGatewayAuthorizationRequestedData ->
                  if (authCompleted.transactionAuthorizationRequestData.paymentMethodName ==
                    NpgClient.PaymentMethod.CARDS.toString()) {
                    authRequestedData.brand
                  } else {
                    authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                  }
                is RedirectTransactionGatewayAuthorizationRequestedData ->
                  authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                else -> null
              }
            paymentMethodName = authCompleted.transactionAuthorizationRequestData.paymentMethodName
            clientId = authCompleted.transactionActivatedData.clientId.effectiveClient.name
            bin = walletBin
            lastFourDigits = walletLastFourDigits
            maskedEmail = walletMaskedEmail
          }
        user = userDto
      }

    return when (authRequestedData.type!!) {
      TransactionGatewayAuthorizationRequestedData.AuthorizationDataType.NPG ->
        when (authCompleted.transactionAuthorizationRequestData.paymentTypeCode) {
          PaymentCode.CP.name ->
            buildAuthorizationCompletedCardClosePaymentRequest(
                authCompleted,
                transactionOutcome,
                transactionId,
                totalAmountEuro,
                feeEuro,
                closePaymentTransactionDetails)
              .cast(ClosePaymentRequestV2Dto::class.java)
          PaymentCode.PPAL.name ->
            buildAuthorizationCompletedPayPalClosePaymentRequest(
                authCompleted,
                transactionOutcome,
                transactionId,
                totalAmountEuro,
                feeEuro,
                closePaymentTransactionDetails)
              .cast(ClosePaymentRequestV2Dto::class.java)
          PaymentCode.BPAY.name ->
            buildAuthorizationCompletedBancomatPayClosePaymentRequest(
                authCompleted,
                transactionOutcome,
                transactionId,
                totalAmountEuro,
                feeEuro,
                closePaymentTransactionDetails)
              .cast(ClosePaymentRequestV2Dto::class.java)
          PaymentCode.MYBK.name ->
            buildAuthorizationCompletedMyBankClosePaymentRequest(
                authCompleted,
                transactionOutcome,
                transactionId,
                totalAmountEuro,
                feeEuro,
                closePaymentTransactionDetails)
              .cast(ClosePaymentRequestV2Dto::class.java)
          PaymentCode.APPL.name ->
            buildAuthorizationCompletedApplePayClosePaymentRequest(
                authCompleted,
                transactionOutcome,
                transactionId,
                totalAmountEuro,
                feeEuro,
                closePaymentTransactionDetails)
              .cast(ClosePaymentRequestV2Dto::class.java)
          PaymentCode.SATY.name ->
            buildAuthorizationCompletedSatispayClosePaymentRequest(
                authCompleted,
                transactionOutcome,
                transactionId,
                totalAmountEuro,
                feeEuro,
                closePaymentTransactionDetails)
              .cast(ClosePaymentRequestV2Dto::class.java)
          else ->
            throw IllegalArgumentException(
              "Unhandled or invalid payment type code: '%s'".format(
                authCompleted.transactionAuthorizationRequestData.paymentTypeCode))
        }
      TransactionGatewayAuthorizationRequestedData.AuthorizationDataType.REDIRECT ->
        Mono.just(
          buildAuthorizationCompletedRedirectClosePaymentRequest(
            authCompleted,
            transactionOutcome,
            transactionId,
            totalAmountEuro,
            feeEuro,
            closePaymentTransactionDetails))
      else ->
        throw IllegalArgumentException(
          "Unhandled or invalid authorization request type: '%s'".format(authRequestedData.type))
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
        idBundle = authCompleted.transactionAuthorizationRequestData.idBundle
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
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
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
  ): Mono<CardClosePaymentRequestV2Dto> {
    val email =
      Mono.defer {
        confidentialDataUtils
          .eCommerceDecrypt(authCompleted.transactionActivatedData.email) { Email(it) }
          .map { it.value }
      }

    val additionalPaymentInformations =
      Mono.just(0)
        .filter { transactionOutcome == ClosePaymentOutcome.OK }
        .flatMap { email }
        .map {
          CardAdditionalPaymentInformationsDto().apply {
            this.outcomePaymentGateway =
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
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
            this.rrn = authCompleted.transactionAuthorizationCompletedData.rrn
            this.totalAmount = totalAmountEuro.toString()
            this.email = it
          }
        }
        .map { Optional.of(it) }
        .defaultIfEmpty(Optional.empty())

    return additionalPaymentInformations.map {
      CardClosePaymentRequestV2Dto().apply {
        this.paymentTokens =
          authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
        this.outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
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
          idBundle = authCompleted.transactionAuthorizationRequestData.idBundle
        }

        this.additionalPaymentInformations = it.orElse(null)
        transactionDetails = closePaymentTransactionDetails
      }
    }
  }

  private fun buildAuthorizationCompletedPayPalClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ): Mono<PayPalClosePaymentRequestV2Dto> {
    val npgTransactionGatewayAuthorizationData =
      authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
        as NpgTransactionGatewayAuthorizationData

    val email =
      Mono.defer {
        confidentialDataUtils
          .eCommerceDecrypt(authCompleted.transactionActivatedData.email) { Email(it) }
          .map { it.value }
      }

    val additionalPaymentInformations =
      Mono.just(0)
        .filter { transactionOutcome == ClosePaymentOutcome.OK }
        .flatMap { email }
        .map {
          PayPalAdditionalPaymentInformationsDto().apply {
            this.transactionId = npgTransactionGatewayAuthorizationData.operationId
            this.pspTransactionId = npgTransactionGatewayAuthorizationData.paymentEndToEndId
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                  authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
            this.totalAmount = totalAmountEuro.toString()
            this.email = it
          }
        }
        .map { Optional.of(it) }
        .defaultIfEmpty(Optional.empty())

    return additionalPaymentInformations.map {
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
          idBundle = authCompleted.transactionAuthorizationRequestData.idBundle
        }

        this.additionalPaymentInformations = it.orElse(null)
        transactionDetails = closePaymentTransactionDetails
      }
    }
  }

  private fun buildAuthorizationCompletedBancomatPayClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ): Mono<BancomatPayClosePaymentRequestV2Dto> {
    val email =
      Mono.defer {
        confidentialDataUtils
          .eCommerceDecrypt(authCompleted.transactionActivatedData.email) { Email(it) }
          .map { it.value }
      }

    val npgTransactionGatewayAuthorizationData =
      authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
        as NpgTransactionGatewayAuthorizationData

    val additionalPaymentInformations =
      Mono.just(0)
        .filter { transactionOutcome == ClosePaymentOutcome.OK }
        .flatMap { email }
        .map {
          BancomatPayAdditionalPaymentInformationsDto().apply {
            this.transactionId = npgTransactionGatewayAuthorizationData.paymentEndToEndId
            this.outcomePaymentGateway =
              BancomatPayAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.valueOf(
                transactionOutcome.name)
            this.totalAmount = totalAmountEuro.toString()
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                  authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
            this.authorizationCode = npgTransactionGatewayAuthorizationData.operationId
            this.email = it
          }
        }
        .map { Optional.of(it) }
        .defaultIfEmpty(Optional.empty())

    return additionalPaymentInformations.map {
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
          idBundle = authCompleted.transactionAuthorizationRequestData.idBundle
        }

        this.additionalPaymentInformations = it.orElse(null)
        transactionDetails = closePaymentTransactionDetails
      }
    }
  }

  private fun buildAuthorizationCompletedApplePayClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ): Mono<ApplePayClosePaymentRequestV2Dto> {
    val email =
      Mono.defer {
        confidentialDataUtils
          .eCommerceDecrypt(authCompleted.transactionActivatedData.email) { Email(it) }
          .map { it.value }
      }

    val additionalPaymentInformations =
      Mono.just(0)
        .filter { transactionOutcome == ClosePaymentOutcome.OK }
        .flatMap { email }
        .map {
          ApplePayAdditionalPaymentInformationsDto().apply {
            this.authorizationRequestId = authCompleted.transactionAuthorizationRequestData.authorizationRequestId
            this.rrn = authCompleted.transactionAuthorizationCompletedData.rrn
            this.totalAmount = totalAmountEuro.toString()
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                  authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
            this.authorizationCode =
              authCompleted.transactionAuthorizationCompletedData.authorizationCode
            this.email = it
          }
        }
        .map { Optional.of(it) }
        .defaultIfEmpty(Optional.empty())

    return additionalPaymentInformations.map {
      ApplePayClosePaymentRequestV2Dto().apply {
        paymentTokens =
          authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
        outcome = ApplePayClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
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
          idBundle = authCompleted.transactionAuthorizationRequestData.idBundle,
        }

        this.additionalPaymentInformations = it.orElse(null)
        transactionDetails = closePaymentTransactionDetails
      }
    }
  }

  private fun buildAuthorizationCompletedSatispayClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ): Mono<SatispayClosePaymentRequestV2Dto> {
    val email =
      Mono.defer {
        confidentialDataUtils
          .eCommerceDecrypt(authCompleted.transactionActivatedData.email) { Email(it) }
          .map { it.value }
      }

    val npgTransactionGatewayAuthorizationData =
      authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
        as NpgTransactionGatewayAuthorizationData

    val additionalPaymentInformations =
      Mono.just(0)
        .filter { transactionOutcome == ClosePaymentOutcome.OK }
        .flatMap { email }
        .map {
          SatispayAdditionalPaymentInformationsDto().apply {
            this.satispayTransactionId = npgTransactionGatewayAuthorizationData.paymentEndToEndId
            this.totalAmount = totalAmountEuro.toString()
            this.fee = feeEuro.toString()
            this.timestampOperation =
              OffsetDateTime.parse(
                  authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
            this.email = it
          }
        }
        .map { Optional.of(it) }
        .defaultIfEmpty(Optional.empty())

    return additionalPaymentInformations.map {
      SatispayClosePaymentRequestV2Dto().apply {
        paymentTokens =
          authCompleted.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken.value }
        outcome = SatispayClosePaymentRequestV2Dto.OutcomeEnum.valueOf(transactionOutcome.name)
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
          idBundle = authCompleted.transactionAuthorizationRequestData.idBundle
        }

        this.additionalPaymentInformations = it.orElse(null)
        transactionDetails = closePaymentTransactionDetails
      }
    }
  }

  private fun buildAuthorizationCompletedMyBankClosePaymentRequest(
    authCompleted: BaseTransactionWithCompletedAuthorization,
    transactionOutcome: ClosePaymentOutcome,
    transactionId: TransactionId,
    totalAmountEuro: BigDecimal,
    feeEuro: BigDecimal,
    closePaymentTransactionDetails: TransactionDetailsDto
  ): Mono<MyBankClosePaymentRequestV2Dto> {
    val email =
      Mono.defer {
        confidentialDataUtils
          .eCommerceDecrypt(authCompleted.transactionActivatedData.email) { Email(it) }
          .map { it.value }
      }

    val npgTransactionGatewayAuthorizationData =
      authCompleted.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
        as NpgTransactionGatewayAuthorizationData

    val additionalPaymentInformations =
      Mono.just(0)
        .filter { transactionOutcome == ClosePaymentOutcome.OK }
        .flatMap { email }
        .map {
          MyBankAdditionalPaymentInformationsDto().apply {
            this.transactionId = authCompleted.transactionId.value()
            this.myBankTransactionId = npgTransactionGatewayAuthorizationData.paymentEndToEndId
            this.totalAmount = totalAmountEuro.toString()
            this.fee = feeEuro.toString()
            this.validationServiceId = npgTransactionGatewayAuthorizationData.validationServiceId
            this.timestampOperation =
              OffsetDateTime.parse(
                  authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(closePaymentZoneId)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toString()
            this.email = it
          }
        }
        .map { Optional.of(it) }
        .defaultIfEmpty(Optional.empty())

    return additionalPaymentInformations.map {
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
          idBundle = authCompleted.transactionAuthorizationRequestData.idBundle
        }

        this.additionalPaymentInformations = it.orElse(null)
        transactionDetails = closePaymentTransactionDetails
      }
    }
  }

  private fun getOutcomePaymentGateway(
    transactionGatewayAuthData: TransactionGatewayAuthorizationData
  ): CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum =
    when (transactionGatewayAuthData) {
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
      is PgsTransactionGatewayAuthorizationData ->
        throw IllegalArgumentException(
          "Unhandled or invalid auth data type 'PgsTransactionGatewayAuthorizationData'")
    }

  private fun getAuthorizationErrorCode(
    transactionGatewayAuthData: TransactionGatewayAuthorizationData
  ): String? =
    when (transactionGatewayAuthData) {
      is NpgTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
      is RedirectTransactionGatewayAuthorizationData -> transactionGatewayAuthData.errorCode
      is PgsTransactionGatewayAuthorizationData ->
        throw IllegalArgumentException(
          "Unhandled or invalid auth data type 'PgsTransactionGatewayAuthorizationData'")
    }

  private fun buildUserInfo(
    baseTransaction: BaseTransactionWithPaymentToken,
    outcome: ClosePaymentOutcome
  ): Mono<UserDto> =
    when (baseTransaction.clientId.effectiveClient) {
      it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId.CHECKOUT,
      it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId.CHECKOUT_CART ->
        mono { UserDto().apply { type = UserDto.TypeEnum.GUEST } }
      it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId.IO -> {
        val userId = baseTransaction.transactionActivatedData.userId
        if (userId != null) {
          if (outcome == ClosePaymentOutcome.OK) {
            confidentialDataUtils.decryptWalletSessionToken(userId).map {
              UserDto().apply {
                type = UserDto.TypeEnum.REGISTERED
                fiscalCode = it
              }
            }
          } else {
            mono { UserDto().apply { type = UserDto.TypeEnum.REGISTERED } }
          }
        } else {
          Mono.error(
            RuntimeException(
              "Invalid user id null for transaction with clientId: [${baseTransaction.clientId}]"))
        }
      }
      else -> Mono.error(RuntimeException("Unhandled client id: [${baseTransaction.clientId}]"))
    }
}
