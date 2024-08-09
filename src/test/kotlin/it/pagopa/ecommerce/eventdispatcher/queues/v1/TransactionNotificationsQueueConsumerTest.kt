package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.TransactionClosureData
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundedData
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.domain.Email
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.ConfidentialDataUtils
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.v1.UserReceiptMailBuilder
import it.pagopa.generated.notifications.templates.success.SuccessTemplate
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono

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

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  @Test
  fun `Should set right value string to payee template name field when TransactionUserReceiptData receivingOfficeName is not null`() =
    runTest {
      val confidentialDataUtils: ConfidentialDataUtils = mock()
      given(confidentialDataUtils.toEmail(any())).willReturn(Email("to@to.it"))
      val userReceiptBuilder = UserReceiptMailBuilder(confidentialDataUtils)
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
      val confidentialDataUtils: ConfidentialDataUtils = mock()
      given(confidentialDataUtils.toEmail(any())).willReturn(Email("to@to.it"))
      val userReceiptBuilder = UserReceiptMailBuilder(confidentialDataUtils)
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
}
