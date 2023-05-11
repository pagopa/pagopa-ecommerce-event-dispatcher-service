package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.domain.v1.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.queues.reduceEvents
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.reactor.awaitSingle
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
                  this.transactionId = transactionId.toString()
                  transactionDetails =
                    TransactionDetailsDto().apply {
                      transaction = TransactionDto().apply {
                        this.transactionId = transactionId.toString()
                        transactionStatus = "Confermato"
                        fee = authCompleted.transactionAuthorizationRequestData.fee.toBigDecimal()
                        amount = authCompleted.transactionAuthorizationRequestData.amount.toBigDecimal()
                        grandTotal = (authCompleted.transactionAuthorizationRequestData.amount.plus(
                          authCompleted.transactionAuthorizationRequestData.fee))
                          .toBigDecimal()
                        rrn = authCompleted.transactionAuthorizationCompletedData.rrn
                        authorizationCode = authCompleted.transactionAuthorizationCompletedData.authorizationCode
                        creationDate = OffsetDateTime.parse(it.creationDate.toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        psp = PspDto().apply {
                          idPsp = authCompleted.transactionAuthorizationRequestData.pspId
                          idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
                          businessName = authCompleted.transactionAuthorizationRequestData.pspBusinessName
                        }
                      }
                      info = InfoDto().apply {
                        type = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                        brandLogo = authCompleted.transactionAuthorizationRequestData.logo.path
                      }
                      user = UserDto().apply {
                        type = UserDto.TypeEnum.GUEST
                      }
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
                        this.transactionId = transactionId.toString()
                        totalAmount =
                          (authCompleted.transactionAuthorizationRequestData.amount.plus(
                              authCompleted.transactionAuthorizationRequestData.fee))
                            .toBigDecimal()
                        fee = authCompleted.transactionAuthorizationRequestData.fee.toBigDecimal()
                        this.timestampOperation =
                          OffsetDateTime.parse(
                            authCompleted.transactionAuthorizationCompletedData.timestampOperation,
                            DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        additionalPaymentInformations =
                          AdditionalPaymentInformationsDto().apply {
                            tipoVersamento = TIPO_VERSAMENTO_CP
                            outcomePaymentGateway =
                              AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.valueOf(
                                authCompleted.transactionAuthorizationCompletedData
                                  .authorizationResultDto
                                  .toString())
                            this.authorizationCode =
                              authCompleted.transactionAuthorizationCompletedData.authorizationCode
                            fee =
                              authCompleted.transactionAuthorizationRequestData.fee.toBigDecimal()
                            this.timestampOperation =
                              OffsetDateTime.parse(
                                authCompleted.transactionAuthorizationCompletedData
                                  .timestampOperation,
                                DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                            rrn = authCompleted.transactionAuthorizationCompletedData.rrn
                          }
                        transactionDetails =
                          TransactionDetailsDto().apply {
                            transaction = TransactionDto().apply {
                              this.transactionId = transactionId.toString()
                              transactionStatus = "Confermato"
                              fee = authCompleted.transactionAuthorizationRequestData.fee.toBigDecimal()
                              amount = authCompleted.transactionAuthorizationRequestData.amount.toBigDecimal()
                              grandTotal = (authCompleted.transactionAuthorizationRequestData.amount.plus(
                                authCompleted.transactionAuthorizationRequestData.fee))
                                .toBigDecimal()
                              rrn = authCompleted.transactionAuthorizationCompletedData.rrn
                              authorizationCode = authCompleted.transactionAuthorizationCompletedData.authorizationCode
                              creationDate = OffsetDateTime.parse(it.creationDate.toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                              psp = PspDto().apply {
                                idPsp = authCompleted.transactionAuthorizationRequestData.pspId
                                idChannel = authCompleted.transactionAuthorizationRequestData.pspChannelCode
                                businessName = authCompleted.transactionAuthorizationRequestData.pspBusinessName
                              }
                            }
                            info = InfoDto().apply {
                              type = authCompleted.transactionAuthorizationRequestData.paymentTypeCode
                              brandLogo = authCompleted.transactionAuthorizationRequestData.logo.path
                            }
                            user = UserDto().apply {
                              type = UserDto.TypeEnum.GUEST
                            }
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
              this.transactionId = transactionId.toString()
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
}
