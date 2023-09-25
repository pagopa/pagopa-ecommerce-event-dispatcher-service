package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.Email
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.*
import it.pagopa.ecommerce.eventdispatcher.utils.v1.UserReceiptMailBuilder
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.notifications.templates.success.SuccessTemplate
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(MockitoExtension::class)
class TransactionNotificationsQueueConsumerTest {

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionUserReceiptRepository:
    TransactionsEventStoreRepository<TransactionUserReceiptData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val checkpointer: Checkpointer = mock()

  private val notificationRetryService: NotificationRetryService = mock()

  private val notificationsServiceClient: NotificationsServiceClient = mock()

  private val userReceiptMailBuilder: UserReceiptMailBuilder = mock()
  private val transactionRefundRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()
  private val paymentGatewayClient: PaymentGatewayClient = mock()
  private val refundRetryService: RefundRetryService = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionUserReceiptCaptor:
    ArgumentCaptor<TransactionEvent<TransactionUserReceiptData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionNotificationsRetryQueueConsumer =
    TransactionNotificationsQueueConsumer(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionUserReceiptRepository = transactionUserReceiptRepository,
      transactionsViewRepository = transactionsViewRepository,
      notificationRetryService = notificationRetryService,
      userReceiptMailBuilder = userReceiptMailBuilder,
      notificationsServiceClient = notificationsServiceClient,
      transactionsRefundedEventStoreRepository = transactionRefundRepository,
      paymentGatewayClient = paymentGatewayClient,
      refundRetryService = refundRetryService,
      deadLetterQueueAsyncClient = deadLetterQueueAsyncClient,
      deadLetterTTLSeconds = DEAD_LETTER_QUEUE_TTL_SECONDS,
      tracingUtils = tracingUtils)

  @Test
  fun `Should successfully send user email for send payment result outcome OK`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
    val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val events =
      listOf(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(),
        transactionAuthorizationCompletedEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        notificationRequested)
        as List<TransactionEvent<Any>>
    val baseTransaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
    val transactionId = TRANSACTION_ID
    val document =
      transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
    Hooks.onOperatorDebug()
    given(checkpointer.success()).willReturn(Mono.empty())
    given(transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
      .willReturn(Flux.fromIterable(events))
    given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
      .willReturn(NotificationEmailRequestDto())
    given(notificationsServiceClient.sendNotificationEmail(any()))
      .willReturn(Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" }))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(document))
    given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor))).willAnswer {
      Mono.just(it.arguments[0])
    }

    StepVerifier.create(
        transactionNotificationsRetryQueueConsumer.messageReceiver(
          notificationRequested to MOCK_TRACING_INFO, checkpointer))
      .expectNext()
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1))
      .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
    verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(transactionsViewRepository, times(1)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(transactionRefundRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionUserReceiptRepository, times(1)).save(any())
    verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
    assertEquals(TransactionStatusDto.NOTIFIED_OK, transactionViewRepositoryCaptor.value.status)
    val savedEvent = transactionUserReceiptCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(transactionUserReceiptData, savedEvent.data)
  }

  @Test
  fun `Should successfully send user email for send payment result outcome KO performing refund for transaction`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" }))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionRefundRepository.save(capture(transactionRefundEventStoreCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(
          Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to MOCK_TRACING_INFO, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
      verify(transactionRefundRepository, times(2)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(3)).save(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      val expectedStatuses =
        listOf(
          TransactionStatusDto.NOTIFIED_KO,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUNDED)
      val expectedEventCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      assertEquals(
        TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
        TransactionEventCode.valueOf(transactionUserReceiptCaptor.value.eventCode))
      assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
      expectedEventCodes.forEachIndexed { index, eventCode ->
        assertEquals(
          eventCode.toString(), transactionRefundEventStoreCaptor.allValues[index].eventCode)
        assertEquals(
          TransactionStatusDto.NOTIFICATION_REQUESTED,
          transactionRefundEventStoreCaptor.allValues[index].data.statusBeforeRefunded)
      }
      expectedStatuses.forEachIndexed { index, transactionStatus ->
        assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
      }
    }

  @Test
  fun `Should successfully send user email for send payment result outcome OK with legacy event`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" }))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }

      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to null, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(TransactionStatusDto.NOTIFIED_OK, transactionViewRepositoryCaptor.value.status)
      val savedEvent = transactionUserReceiptCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
        TransactionEventCode.valueOf(savedEvent.eventCode))
      assertEquals(transactionUserReceiptData, savedEvent.data)
    }

  @Test
  fun `Should successfully send user email for send payment result outcome KO performing refund for transaction with legacy event`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" }))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionRefundRepository.save(capture(transactionRefundEventStoreCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(
          Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to null, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
      verify(transactionRefundRepository, times(2)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(3)).save(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      val expectedStatuses =
        listOf(
          TransactionStatusDto.NOTIFIED_KO,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUNDED)
      val expectedEventCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      assertEquals(
        TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
        TransactionEventCode.valueOf(transactionUserReceiptCaptor.value.eventCode))
      assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
      expectedEventCodes.forEachIndexed { index, eventCode ->
        assertEquals(
          eventCode.toString(), transactionRefundEventStoreCaptor.allValues[index].eventCode)
        assertEquals(
          TransactionStatusDto.NOTIFICATION_REQUESTED,
          transactionRefundEventStoreCaptor.allValues[index].data.statusBeforeRefunded)
      }
      expectedStatuses.forEachIndexed { index, transactionStatus ->
        assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
      }
    }

  @Test
  fun `Should successfully send user email for send payment result outcome KO and enqueue refund retry event in case of error performing refund`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" }))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionRefundRepository.save(capture(transactionRefundEventStoreCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.error(RuntimeException("Error performing refunding")))
      given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to MOCK_TRACING_INFO, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(transactionRefundRepository, times(2)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsViewRepository, times(3)).save(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      val expectedStatuses =
        listOf(
          TransactionStatusDto.NOTIFIED_KO,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUND_ERROR)
      val expectedEventCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT)
      assertEquals(
        TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
        TransactionEventCode.valueOf(transactionUserReceiptCaptor.value.eventCode))
      assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
      expectedEventCodes.forEachIndexed { index, eventCode ->
        assertEquals(
          eventCode.toString(), transactionRefundEventStoreCaptor.allValues[index].eventCode)
        assertEquals(
          TransactionStatusDto.NOTIFICATION_REQUESTED,
          transactionRefundEventStoreCaptor.allValues[index].data.statusBeforeRefunded)
      }
      expectedStatuses.forEachIndexed { index, transactionStatus ->
        assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
      }
    }

  @Test
  fun `Should enqueue notification retry event for failure calling notification service send payment result outcome OK`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.error(RuntimeException("Error calling notification service")))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor), any()))
        .willReturn(Mono.empty())
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to MOCK_TRACING_INFO, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)

      assertEquals(0, retryCountCaptor.value)
      assertEquals(
        TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT,
        TransactionEventCode.valueOf(transactionUserReceiptCaptor.value.eventCode))
      assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, transactionViewRepositoryCaptor.value.status)
    }

  @Test
  fun `Should enqueue notification retry event for failure calling notification service send payment result outcome KO`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.error(RuntimeException("Error calling notification service")))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor), any()))
        .willReturn(Mono.empty())
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to MOCK_TRACING_INFO, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(0, retryCountCaptor.value)
      val expectedStatuses =
        listOf(
          TransactionStatusDto.NOTIFICATION_ERROR,
        )
      val expectedEventCodes = listOf(TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT)
      expectedEventCodes.forEachIndexed { index, eventCode ->
        assertEquals(eventCode.toString(), transactionUserReceiptCaptor.allValues[index].eventCode)
        assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.allValues[index].data)
      }
      expectedStatuses.forEachIndexed { index, transactionStatus ->
        assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
      }
    }

  @Test
  fun `Should return mono error for failure enqueuing notification retry event send payment result OK`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.error(RuntimeException("Error calling notification service")))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor), any()))
        .willReturn(Mono.error(RuntimeException("Error enqueueing notification retry event")))
      given(
          deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())

      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to MOCK_TRACING_INFO, checkpointer))
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(0, retryCountCaptor.value)
      val expectedStatuses =
        listOf(
          TransactionStatusDto.NOTIFICATION_ERROR,
        )
      val expectedEventCodes = listOf(TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT)
      expectedEventCodes.forEachIndexed { index, eventCode ->
        assertEquals(eventCode.toString(), transactionUserReceiptCaptor.allValues[index].eventCode)
        assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.allValues[index].data)
      }
      expectedStatuses.forEachIndexed { index, transactionStatus ->
        assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
      }
      verify(deadLetterQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionUserReceiptRequestedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT
          },
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
    }

  @Test
  fun `Should not process event for wrong transaction status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
    val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val events = listOf(transactionActivateEvent()) as List<TransactionEvent<Any>>
    val transactionId = TRANSACTION_ID
    Hooks.onOperatorDebug()
    given(checkpointer.success()).willReturn(Mono.empty())
    given(transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
      .willReturn(Flux.fromIterable(events))
    given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())

    StepVerifier.create(
        transactionNotificationsRetryQueueConsumer.messageReceiver(
          notificationRequested to MOCK_TRACING_INFO, checkpointer))
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1))
      .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
    verify(notificationsServiceClient, times(0)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(transactionsViewRepository, times(0)).save(any())
    verify(transactionRefundRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionUserReceiptRepository, times(0)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(userReceiptMailBuilder, times(0)).buildNotificationEmailRequestDto(any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(
                object : TypeReference<QueueEvent<TransactionUserReceiptRequestedEvent>>() {})
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT
        },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `Should set right value string to payee template name field when TransactionUserReceiptData receivingOfficeName is not null`() =
    runTest {
      val confidentialMailUtils: ConfidentialMailUtils = mock()
      given(confidentialMailUtils.toEmail(any())).willReturn(Email("to@to.it"))
      val userReceiptBuilder = UserReceiptMailBuilder(confidentialMailUtils)
      val transactionUserReceiptData =
        TransactionUserReceiptData(
          TransactionUserReceiptData.Outcome.OK,
          "it-IT",
          PAYMENT_DATE,
          "testValue",
          "paymentDescription")
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
        .willReturn(Flux.fromIterable(events))

      val notificationEmailRequestDto =
        userReceiptBuilder.buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(
        "testValue",
        (notificationEmailRequestDto.parameters as SuccessTemplate)
          .cart
          .items
          .filter { i -> i.payee != null }[0]
          .payee
          .name)
    }

  @Test
  fun `Should set empty string to payee template name field when TransactionUserReceiptData receivingOfficeName is null`() =
    runTest {
      val confidentialMailUtils: ConfidentialMailUtils = mock()
      given(confidentialMailUtils.toEmail(any())).willReturn(Email("to@to.it"))
      val userReceiptBuilder = UserReceiptMailBuilder(confidentialMailUtils)
      val transactionUserReceiptData =
        TransactionUserReceiptData(
          TransactionUserReceiptData.Outcome.OK, "it-IT", PAYMENT_DATE, null, "paymentDescription")
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId))
        .willReturn(Flux.fromIterable(events))

      val notificationEmailRequestDto =
        userReceiptBuilder.buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(
        "",
        (notificationEmailRequestDto.parameters as SuccessTemplate)
          .cart
          .items
          .filter { i -> i.payee != null }[0]
          .payee
          .name)
    }

  @Test
  fun `Should not process event for transaction with invalid send payment result outcome`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.NOT_RECEIVED)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_REQUESTED, ZonedDateTime.now())
      Hooks.onOperatorDebug()
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(Flux.fromIterable(events))
      given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
        .willReturn(NotificationEmailRequestDto())
      given(notificationsServiceClient.sendNotificationEmail(any()))
        .willReturn(Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" }))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(document))
      given(transactionsViewRepository.save(capture(transactionViewRepositoryCaptor))).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionUserReceiptRepository.save(capture(transactionUserReceiptCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor), any()))
        .willReturn(Mono.empty())
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            notificationRequested to MOCK_TRACING_INFO, checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)

      assertEquals(0, retryCountCaptor.value)
      assertEquals(
        TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT,
        TransactionEventCode.valueOf(transactionUserReceiptCaptor.value.eventCode))
      assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, transactionViewRepositoryCaptor.value.status)
    }
}
