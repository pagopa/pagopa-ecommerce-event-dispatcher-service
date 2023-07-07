package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import com.fasterxml.jackson.core.type.TypeReference
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DEAD_LETTER_QUEUE_TTL_SECONDS
import it.pagopa.ecommerce.eventdispatcher.utils.OBJECT_MAPPER
import it.pagopa.ecommerce.eventdispatcher.utils.UserReceiptMailBuilder
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
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
class TransactionNotificationsRetryQueueConsumerTest {
  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionUserReceiptRepository:
    TransactionsEventStoreRepository<TransactionUserReceiptData> =
    mock()

  private val transactionRefundRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val checkpointer: Checkpointer = mock()

  private val notificationRetryService: NotificationRetryService = mock()

  private val refundRetryService: RefundRetryService = mock()

  private val notificationsServiceClient: NotificationsServiceClient = mock()

  private val userReceiptMailBuilder: UserReceiptMailBuilder = mock()

  private val openTelemetry: OpenTelemetry = mock()

  private val tracer: Tracer = mock()

  private val objectMapper = OBJECT_MAPPER

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  @Captor
  private lateinit var transactionUserReceiptCaptor:
    ArgumentCaptor<TransactionEvent<TransactionUserReceiptData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionNotificationsRetryQueueConsumer =
    TransactionNotificationsRetryQueueConsumer(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionUserReceiptRepository = transactionUserReceiptRepository,
      transactionsViewRepository = transactionsViewRepository,
      notificationRetryService = notificationRetryService,
      transactionsRefundedEventStoreRepository = transactionRefundRepository,
      paymentGatewayClient = paymentGatewayClient,
      refundRetryService = refundRetryService,
      userReceiptMailBuilder = userReceiptMailBuilder,
      notificationsServiceClient = notificationsServiceClient,
      deadLetterQueueAsyncClient = deadLetterQueueAsyncClient,
      deadLetterTTLSeconds = DEAD_LETTER_QUEUE_TTL_SECONDS,
      openTelemetry,
      tracer,
      objectMapper)

  @BeforeEach
  fun setupTracingMocks() = it.pagopa.ecommerce.eventdispatcher.utils.setupTracingMocks()

  @Test
  fun `Should successfully retry send user email for send payment result outcome OK`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
    val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
    val events =
      listOf(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(),
        transactionAuthorizationCompletedEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(transactionUserReceiptData),
        notificationErrorEvent)
        as List<TransactionEvent<Any>>
    val baseTransaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
    val transactionId = TRANSACTION_ID
    val document = transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
          objectMapper.writeValueAsBytes(QueueEvent(notificationErrorEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext()
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1))
      .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
    verify(transactionUserReceiptRepository, times(1)).save(any())
    verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(transactionsViewRepository, times(1)).save(any())
    verify(transactionRefundRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
    assertEquals(TransactionStatusDto.NOTIFIED_OK, transactionViewRepositoryCaptor.value.status)
    val savedEvent = transactionUserReceiptCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT, savedEvent.eventCode)
    assertEquals(transactionUserReceiptData, savedEvent.data)
  }

  @Test
  fun `Should successfully retry send user email for send payment result outcome KO`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
    val events =
      listOf(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(),
        transactionAuthorizationCompletedEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(transactionUserReceiptData),
        notificationErrorEvent)
        as List<TransactionEvent<Any>>
    val baseTransaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
    val transactionId = TRANSACTION_ID
    val document = transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
    given(transactionRefundRepository.save(capture(transactionRefundEventStoreCaptor))).willAnswer {
      Mono.just(it.arguments[0])
    }
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
          objectMapper.writeValueAsBytes(QueueEvent(notificationErrorEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext()
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1))
      .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
    verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(transactionsViewRepository, times(3)).save(any())
    verify(transactionRefundRepository, times(2)).save(any())
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
    verify(transactionUserReceiptRepository, times(1)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
    val savedEvent = transactionUserReceiptCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT, savedEvent.eventCode)
    assertEquals(transactionUserReceiptData, savedEvent.data)
    val expectedStatuses =
      listOf(
        TransactionStatusDto.NOTIFIED_KO,
        TransactionStatusDto.REFUND_REQUESTED,
        TransactionStatusDto.REFUNDED)
    val expectedEventCodes =
      listOf(
        TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
    expectedEventCodes.forEachIndexed { index, eventCode ->
      assertEquals(eventCode, transactionRefundEventStoreCaptor.allValues[index].eventCode)
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR,
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
      val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          transactionUserReceiptRequestedEvent(transactionUserReceiptData),
          notificationErrorEvent)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor)))
        .willReturn(Mono.empty())
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            objectMapper.writeValueAsBytes(QueueEvent(notificationErrorEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)

      assertEquals(0, retryCountCaptor.value)
    }

  @Test
  fun `Should enqueue notification retry event for failure calling notification service send payment result outcome KO`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          transactionUserReceiptRequestedEvent(transactionUserReceiptData),
          notificationErrorEvent)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor)))
        .willReturn(Mono.empty())
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            objectMapper.writeValueAsBytes(QueueEvent(notificationErrorEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(0, retryCountCaptor.value)
    }

  @Test
  fun `Should enqueue notification retry event for failure retry calling notification service send payment result outcome OK`() =
    runTest {
      val attempt = 1
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val notificationRetriedEvent = transactionUserReceiptAddRetriedEvent(attempt)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          transactionUserReceiptRequestedEvent(transactionUserReceiptData),
          notificationErrorEvent,
          notificationRetriedEvent)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor)))
        .willReturn(Mono.empty())
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            objectMapper.writeValueAsBytes(QueueEvent(notificationRetriedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(attempt, retryCountCaptor.value)
    }

  @Test
  fun `Should enqueue notification retry event for failure retry calling notification service send payment result outcome KO`() =
    runTest {
      val attempt = 1
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val notificationRetriedEvent = transactionUserReceiptAddRetriedEvent(attempt)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          transactionUserReceiptRequestedEvent(transactionUserReceiptData),
          notificationErrorEvent,
          notificationRetriedEvent)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor)))
        .willReturn(Mono.empty())

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
            objectMapper.writeValueAsBytes(QueueEvent(notificationRetriedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(0)).save(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(attempt, retryCountCaptor.value)
    }

  @Test
  fun `Should not perform refund for no left attempts resending mail for send payment result OK`() =
    runTest {
      val attempt = 3
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val notificationRetriedEvent = transactionUserReceiptAddRetriedEvent(attempt)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          transactionUserReceiptRequestedEvent(transactionUserReceiptData),
          notificationErrorEvent,
          notificationRetriedEvent)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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

      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor)))
        .willReturn(
          Mono.error(
            NoRetryAttemptsLeftException(
              TransactionId(transactionId),
              TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT)))

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
            objectMapper.writeValueAsBytes(QueueEvent(notificationRetriedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionRefundRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(attempt, retryCountCaptor.value)
    }

  @Test
  fun `Should perform refund for no left attempts resending mail for send payment result KO`() =
    runTest {
      val attempt = 3
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val notificationRetriedEvent = transactionUserReceiptAddRetriedEvent(attempt)
      val events =
        listOf(
          transactionActivateEvent(),
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          transactionUserReceiptRequestedEvent(transactionUserReceiptData),
          notificationErrorEvent,
          notificationRetriedEvent)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val transactionId = TRANSACTION_ID
      val document =
        transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
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
      given(notificationRetryService.enqueueRetryEvent(any(), capture(retryCountCaptor)))
        .willReturn(
          Mono.error(
            NoRetryAttemptsLeftException(
              TransactionId(transactionId),
              TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT)))
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
            objectMapper.writeValueAsBytes(QueueEvent(notificationRetriedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1))
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(2)).save(any())
      verify(transactionRefundRepository, times(2)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionUserReceiptRepository, times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(attempt, retryCountCaptor.value)
      val expectedStatuses =
        listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
      val expectedEventCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      expectedEventCodes.forEachIndexed { index, eventCode ->
        assertEquals(eventCode, transactionRefundEventStoreCaptor.allValues[index].eventCode)
        assertEquals(
          TransactionStatusDto.NOTIFICATION_ERROR,
          transactionRefundEventStoreCaptor.allValues[index].data.statusBeforeRefunded)
      }
      expectedStatuses.forEachIndexed { index, transactionStatus ->
        assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
      }
    }

  @Test
  fun `Should not process event for wrong transaction status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
    val notificationErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
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
          objectMapper.writeValueAsBytes(QueueEvent(notificationErrorEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1))
      .findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID)
    verify(notificationsServiceClient, times(0)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(transactionsViewRepository, times(0)).save(any())
    verify(transactionRefundRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionUserReceiptRepository, times(0)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(userReceiptMailBuilder, times(0)).buildNotificationEmailRequestDto(any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> {
          objectMapper
            .readValue(
              this.toBytes(),
              object : TypeReference<QueueEvent<TransactionUserReceiptAddErrorEvent>>() {})
            .event
            .eventCode == TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT
        },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
  }
}
