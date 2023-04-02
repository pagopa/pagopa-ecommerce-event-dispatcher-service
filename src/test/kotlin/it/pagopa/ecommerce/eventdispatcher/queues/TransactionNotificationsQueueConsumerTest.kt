package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.UserReceiptMailBuilder
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
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

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionUserReceiptCaptor:
    ArgumentCaptor<TransactionEvent<TransactionUserReceiptData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val transactionNotificationsRetryQueueConsumer =
    TransactionNotificationsQueueConsumer(
      transactionsEventStoreRepository,
      transactionUserReceiptRepository,
      transactionsViewRepository,
      notificationRetryService,
      userReceiptMailBuilder,
      notificationsServiceClient)

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
    given(transactionsEventStoreRepository.findByTransactionId(transactionId))
      .willReturn(Flux.fromIterable(events))
    given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
      .willReturn(NotificationEmailRequestDto())
    given(notificationsServiceClient.sendNotificationEmail(any()))
      .willReturn(Mono.just(NotificationEmailResponseDto().outcome("OK")))
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
          BinaryData.fromObject(notificationRequested).toBytes(), checkpointer))
      .expectNext()
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
    verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(transactionsViewRepository, times(1)).save(any())
    assertEquals(TransactionStatusDto.NOTIFIED_OK, transactionViewRepositoryCaptor.value.status)
    val savedEvent = transactionUserReceiptCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT, savedEvent.eventCode)
    assertEquals(transactionUserReceiptData, savedEvent.data)
  }

  @Test
  fun `Should successfully send user email for send payment result outcome KO`() = runTest {
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
    given(transactionsEventStoreRepository.findByTransactionId(transactionId))
      .willReturn(Flux.fromIterable(events))
    given(userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction))
      .willReturn(NotificationEmailRequestDto())
    given(notificationsServiceClient.sendNotificationEmail(any()))
      .willReturn(Mono.just(NotificationEmailResponseDto().outcome("OK")))
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
          BinaryData.fromObject(notificationRequested).toBytes(), checkpointer))
      .expectNext()
      .verifyComplete()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
    verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(transactionsViewRepository, times(1)).save(any())
    assertEquals(TransactionStatusDto.NOTIFIED_KO, transactionViewRepositoryCaptor.value.status)
    val savedEvent = transactionUserReceiptCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT, savedEvent.eventCode)
    assertEquals(transactionUserReceiptData, savedEvent.data)
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
      given(transactionsEventStoreRepository.findByTransactionId(any()))
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
            BinaryData.fromObject(notificationRequested).toBytes(), checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1)).findByTransactionId(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      assertEquals(0, retryCountCaptor.value)
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
      given(transactionsEventStoreRepository.findByTransactionId(any()))
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
            BinaryData.fromObject(notificationRequested).toBytes(), checkpointer))
        .expectNext()
        .verifyComplete()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1)).findByTransactionId(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      assertEquals(0, retryCountCaptor.value)
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
      given(transactionsEventStoreRepository.findByTransactionId(any()))
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
        .willReturn(Mono.error(RuntimeException("Error enqueueing notification retry event")))
      StepVerifier.create(
          transactionNotificationsRetryQueueConsumer.messageReceiver(
            BinaryData.fromObject(notificationRequested).toBytes(), checkpointer))
        .expectError(RuntimeException::class.java)
        .verify()
      verify(checkpointer, times(1)).success()
      verify(transactionsEventStoreRepository, times(1)).findByTransactionId(transactionId)
      verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
      verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(transactionsViewRepository, times(0)).save(any())
      assertEquals(0, retryCountCaptor.value)
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
    given(transactionsEventStoreRepository.findByTransactionId(transactionId))
      .willReturn(Flux.fromIterable(events))

    StepVerifier.create(
        transactionNotificationsRetryQueueConsumer.messageReceiver(
          BinaryData.fromObject(notificationRequested).toBytes(), checkpointer))
      .expectError(BadTransactionStatusException::class.java)
      .verify()
    verify(checkpointer, times(1)).success()
    verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
    verify(notificationsServiceClient, times(0)).sendNotificationEmail(any())
    verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(transactionsViewRepository, times(0)).save(any())
  }
}
