package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.EuroUtils
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

const val TIPO_VERSAMENTO_CP = "CP"

enum class TransactionDetailsStatusEnum(val status: String) {
  TRANSACTION_DETAILS_STATUS_CANCELED("Annullato"),
  TRANSACTION_DETAILS_STATUS_AUTHORIZED("Autorizzato"),
  TRANSACTION_DETAILS_STATUS_DENIED("Rifiutato")
}

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

    val baseTransaction =
      reduceEvents(
        Mono.just(transactionId.value()), transactionsEventStoreRepository, EmptyTransaction())

    val closePaymentRequest =
      baseTransaction.map {
        when (it.status) {
          TransactionStatusDto.CLOSURE_ERROR -> {
            when (transactionOutcome) {
              ClosePaymentRequestV2Dto.OutcomeEnum.KO ->
                ClosePaymentRequestV2Dto().apply {
                  paymentTokens = it.paymentNotices.map { el -> el.paymentToken.value }
                  outcome = transactionOutcome
                  this.transactionId = transactionId.value()
                  transactionDetails =
                    TransactionDetailsDto().apply {
                      transaction =
                        TransactionDto().apply {
                          this.transactionId = transactionId.value()
                          transactionStatus = getTransactionDetailsStatus(it)
                          creationDate = it.creationDate.toOffsetDateTime()
                        }
                      info = InfoDto().apply { type = getPaymentTypeCode(it) }
                      user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
                    }
                }
              ClosePaymentRequestV2Dto.OutcomeEnum.OK ->
                if (it is TransactionWithClosureError) {
                  val transactionAtPreviousState = it.transactionAtPreviousState()
                  transactionAtPreviousState
                    .map { event ->
                      val authCompleted = event.get()
                      ClosePaymentRequestV2Dto().apply {
                        paymentTokens =
                          authCompleted.paymentNotices.map { paymentNotice ->
                            paymentNotice.paymentToken.value
                          }
                        outcome = transactionOutcome
                        idPSP = authCompleted.transactionAuthorizationRequestData.pspId
                        paymentMethod =
                          authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                        idBrokerPSP = authCompleted.transactionAuthorizationRequestData.brokerName
                        idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
                        this.transactionId = transactionId.value()
                        totalAmount =
                          EuroUtils.euroCentsToEuro(
                            (authCompleted.transactionAuthorizationRequestData.amount.plus(
                              authCompleted.transactionAuthorizationRequestData.fee)))
                        fee = authCompleted.transactionAuthorizationRequestData.fee.toBigDecimal()
                        this.timestampOperation =
                          OffsetDateTime.parse(
                            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        additionalPaymentInformations =
                          AdditionalPaymentInformationsDto().apply {
                            outcomePaymentGateway =
                              AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.valueOf(
                                authCompleted.transactionAuthorizationCompletedData
                                  .authorizationResultDto
                                  .toString())
                            this.authorizationCode =
                              authCompleted.transactionAuthorizationCompletedData.authorizationCode
                            fee =
                              EuroUtils.euroCentsToEuro(
                                  authCompleted.transactionAuthorizationRequestData.fee)
                                .toString()
                            this.timestampOperation =
                              OffsetDateTime.parse(
                                  authCompleted.transactionAuthorizationCompletedData
                                    .timestampOperation,
                                  DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                                .truncatedTo(ChronoUnit.SECONDS)
                                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                            this.rrn = getRrnForClosePaymentByAuthorization(authCompleted.transactionAuthorizationRequestData.paymentGateway,
                              authCompleted.transactionAuthorizationCompletedData.rrn!!
                            )
                            this.totalAmount =
                              EuroUtils.euroCentsToEuro(
                                  (authCompleted.transactionAuthorizationRequestData.amount.plus(
                                    authCompleted.transactionAuthorizationRequestData.fee)))
                                .toString()
                          }
                        transactionDetails =
                          TransactionDetailsDto().apply {
                            transaction =
                              TransactionDto().apply {
                                this.transactionId = transactionId.value()
                                transactionStatus = getTransactionDetailsStatus(it)
                                fee =
                                  authCompleted.transactionAuthorizationRequestData.fee
                                    .toBigDecimal()
                                amount =
                                  authCompleted.transactionAuthorizationRequestData.amount
                                    .toBigDecimal()
                                grandTotal =
                                  (authCompleted.transactionAuthorizationRequestData.amount.plus(
                                      authCompleted.transactionAuthorizationRequestData.fee))
                                    .toBigDecimal()
                                rrn = authCompleted.transactionAuthorizationCompletedData.rrn
                                authorizationCode =
                                  authCompleted.transactionAuthorizationCompletedData
                                    .authorizationCode
                                creationDate = it.creationDate.toOffsetDateTime()
                                psp =
                                  PspDto().apply {
                                    idPsp = authCompleted.transactionAuthorizationRequestData.pspId
                                    idChannel =
                                      authCompleted.transactionAuthorizationRequestData
                                        .pspChannelCode
                                    businessName =
                                      authCompleted.transactionAuthorizationRequestData
                                        .pspBusinessName
                                  }
                              }
                            info =
                              InfoDto().apply {
                                type =
                                  authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                                brandLogo =
                                  authCompleted.transactionAuthorizationRequestData.logo.toString()
                              }
                            user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
                          }
                      }
                    }
                    .orElseThrow {
                      RuntimeException(
                        "Unexpected transactionAtPreviousStep: ${it.transactionAtPreviousState}")
                    }
                } else {
                  throw RuntimeException(
                    "Unexpected error while casting request into TransactionWithClosureError")
                }
            }
          }
          TransactionStatusDto.CANCELLATION_REQUESTED ->
            ClosePaymentRequestV2Dto().apply {
              paymentTokens = it.paymentNotices.map { el -> el.paymentToken.value }
              outcome = transactionOutcome
              this.transactionId = transactionId.value()
              transactionDetails =
                TransactionDetailsDto().apply {
                  transaction =
                    TransactionDto().apply {
                      this.transactionId = transactionId.value()
                      transactionStatus = getTransactionDetailsStatus(it)
                      creationDate = it.creationDate.toOffsetDateTime()
                    }
                  info = InfoDto().apply { type = "CP" }
                  user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
                }
            }
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
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_AUTHORIZED.status
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
}


private fun getRrnForClosePaymentByAuthorization(
  paymentGatewayFromAuthorization: TransactionAuthorizationRequestData.PaymentGateway,
  rrnFromAuthorizationCompletedData: String
): String {
  val xpayRrnLength = 7
  return if (paymentGatewayFromAuthorization == TransactionAuthorizationRequestData.PaymentGateway.XPAY
    && rrnFromAuthorizationCompletedData != null && rrnFromAuthorizationCompletedData.length > xpayRrnLength
  ) rrnFromAuthorizationCompletedData.substring(0, xpayRrnLength) else rrnFromAuthorizationCompletedData
}
