package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationRequestedData
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.Transaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.UserStatsServiceClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.userstats.dto.GuestMethodLastUsageData
import it.pagopa.generated.ecommerce.userstats.dto.WalletLastUsageData
import java.time.Duration
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * This helper implements the business logic related to handling calling `getState` from NPG. In
 * particular, the [getAuthorizationState] method does the following:
 * - checks for the transaction current status
 * - determines whether the transaction was requesting authorization via NPG
 * - calls NPG's `getSTate`
 * - calls transactions-service PATCH auth-request
 * - enqueues a retry event in case of error
 */
@Component
class AuthorizationRequestedHelper(
  @Autowired private val transactionsServiceClient: TransactionsServiceClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService,
  @Autowired private val authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val userStatsServiceClient: UserStatsServiceClient,
  @Value("\${userStatsService.enableSaveLastUsage}") private val enableSaveLastMethodUsage: Boolean,
  @Autowired private val authRequestedQueueAsyncClient: QueueAsyncClient,
  @Value("\${transactionAuthorizationOutcomeWaiting.firstAttemptDelaySeconds}")
  private val firstAttemptDelaySeconds: Int,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueueTTLSeconds: Int,
) {

  var logger: Logger = LoggerFactory.getLogger(AuthorizationRequestedHelper::class.java)

  private fun isWalletPayment(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
      .transactionGatewayAuthorizationRequestedData) is
      NpgTransactionGatewayAuthorizationRequestedData &&
      (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
          .transactionGatewayAuthorizationRequestedData
          as NpgTransactionGatewayAuthorizationRequestedData)
        .walletInfo != null

  private fun getWalletIdPayment(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
        .transactionGatewayAuthorizationRequestedData
        as NpgTransactionGatewayAuthorizationRequestedData)
      .walletInfo
      ?.walletId

  private fun getPaymentMethodId(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData)
      .paymentInstrumentId

  fun authorizationRequestedHandler(
    parsedEvent: QueueEvent<TransactionAuthorizationRequestedEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val tracingInfo = parsedEvent.tracingInfo
    val transactionId = parsedEvent.event.transactionId
    val authorizationRequestedDate =
      OffsetDateTime.parse(parsedEvent.event.creationDate, DateTimeFormatter.ISO_DATE_TIME)
    val transaction =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .filter { it is BaseTransactionWithRequestedAuthorization }
        .cast(BaseTransactionWithRequestedAuthorization::class.java)
    val getStateThresholdDate =
      authorizationRequestedDate + Duration.ofSeconds(firstAttemptDelaySeconds.toLong())
    val now = OffsetDateTime.now()
    val timeToWaitForGetState = Duration.between(now, getStateThresholdDate)
    val isFirstAttempt = timeToWaitForGetState >= Duration.ZERO
    val saveLastUsage = enableSaveLastMethodUsage && isFirstAttempt
    val authorizationRequestedPipeline =
      transaction
        .flatMap { baseTransactionWithRequestedAuthorization ->
          if (saveLastUsage &&
            isAuthenticatedTransaction(baseTransactionWithRequestedAuthorization)) {
            userStatsServiceClient
              .saveLastUsage(
                UUID.fromString(
                  baseTransactionWithRequestedAuthorization.transactionActivatedData.userId!!),
                buildUserLastPaymentMethodData(
                  baseTransactionWithRequestedAuthorization, authorizationRequestedDate))
              .onErrorResume {
                logger.error("Exception while saving last payment method used", it)
                mono {}
              }
              .thenReturn(baseTransactionWithRequestedAuthorization)
          } else {
            mono { baseTransactionWithRequestedAuthorization }
          }
        }
        .filter {
          val transactionStatus = it.status
          val gateway = it.transactionAuthorizationRequestData.paymentGateway
          // perform get state operation iff transaction is in AUTHORIZATION_REQUESTED state and the
          // gateway is NPG
          val performGetState =
            transactionStatus == TransactionStatusDto.AUTHORIZATION_REQUESTED &&
              gateway == TransactionAuthorizationRequestData.PaymentGateway.NPG

          logger.info(
            "Transaction [{}] status: [{}], gateway: [{}]. Perform get state: [{}]",
            transactionId,
            transactionStatus,
            gateway,
            performGetState)
          performGetState
        }
        .flatMap { tx ->
          logger.info(
            "Transaction [{}] auth requested at: [{}], get state threshold: [{}]",
            tx.transactionId.value(),
            authorizationRequestedDate,
            getStateThresholdDate)
          if (timeToWaitForGetState > Duration.ZERO) {
            val binaryData =
              BinaryData.fromObject(parsedEvent, strictSerializerProviderV2.createInstance())
            authRequestedQueueAsyncClient.sendMessageWithResponse(
              binaryData,
              // add here a fixed 10 sec delay to avoid condition when event is visible in queue
              // some millis before the effective ttl set here
              timeToWaitForGetState + Duration.ofSeconds(10), // visibility timeout
              Duration.ofSeconds(transientQueueTTLSeconds.toLong()), // ttl
            )
          } else {
            handleGetStateByPatchTransactionService(
              tx = tx,
              authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
              authorizationStateRetrieverService = authorizationStateRetrieverService,
              transactionsServiceClient = transactionsServiceClient,
              tracingInfo = tracingInfo,
              retryCount = 0)
          }
        }
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      authorizationRequestedPipeline,
      QueueEvent(parsedEvent.event, tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  fun authorizationOutcomeWaitingHandler(
    parsedEvent: QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val tracingInfo = parsedEvent.tracingInfo
    val transactionId = parsedEvent.event.transactionId
    val retryCount = parsedEvent.event.data.retryCount

    val transaction =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .filter { it is BaseTransactionWithRequestedAuthorization }
        .cast(BaseTransactionWithRequestedAuthorization::class.java)

    val authorizationRequestedPipeline =
      transaction
        .filter {
          val transactionStatus = it.status
          val gateway = it.transactionAuthorizationRequestData.paymentGateway
          // perform get state operation iff transaction is in AUTHORIZATION_REQUESTED state and the
          // gateway is NPG, and it's a retry event (the first event has visibility timeout and
          // perform save last usage if needed)
          val performGetState =
            transactionStatus == TransactionStatusDto.AUTHORIZATION_REQUESTED &&
              gateway == TransactionAuthorizationRequestData.PaymentGateway.NPG

          logger.info(
            "Transaction [{}}] status: [{}], gateway: [{}]- Perform GET state -> [{}]",
            transactionId,
            transactionStatus,
            gateway,
            performGetState)
          performGetState
        }
        .doOnNext {
          logger.info(
            "Handling get state request for transaction with id ${it.transactionId.value()}")
        }
        .flatMap { tx ->
          handleGetStateByPatchTransactionService(
            tx = tx,
            authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
            authorizationStateRetrieverService = authorizationStateRetrieverService,
            transactionsServiceClient = transactionsServiceClient,
            tracingInfo = tracingInfo,
            retryCount = retryCount)
        }
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      authorizationRequestedPipeline,
      QueueEvent(parsedEvent.event, tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  private fun buildUserLastPaymentMethodData(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization,
    creationDate: OffsetDateTime
  ) =
    when (isWalletPayment(baseTransactionWithRequestedAuthorization)) {
      true ->
        WalletLastUsageData()
          .walletId(UUID.fromString(getWalletIdPayment(baseTransactionWithRequestedAuthorization)))
          .date(creationDate)
      false ->
        GuestMethodLastUsageData()
          .paymentMethodId(
            UUID.fromString(getPaymentMethodId(baseTransactionWithRequestedAuthorization)))
          .date(creationDate)
    }

  private fun isAuthenticatedTransaction(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) = baseTransactionWithRequestedAuthorization.transactionActivatedData.userId != null
}
