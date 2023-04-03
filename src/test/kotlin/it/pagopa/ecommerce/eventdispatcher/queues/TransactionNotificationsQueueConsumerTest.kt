package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.UserReceiptMailBuilder
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
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
import java.time.ZonedDateTime
import java.util.*

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

    @Captor
    private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

    @Captor
    private lateinit var transactionUserReceiptCaptor:
            ArgumentCaptor<TransactionEvent<TransactionUserReceiptData>>

    @Captor
    private lateinit var retryCountCaptor: ArgumentCaptor<Int>

    @Captor
    private lateinit var transactionRefundEventStoreCaptor:
            ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

    private val transactionNotificationsRetryQueueConsumer =
        TransactionNotificationsQueueConsumer(
            transactionsEventStoreRepository,
            transactionUserReceiptRepository,
            transactionsViewRepository,
            notificationRetryService,
            userReceiptMailBuilder,
            notificationsServiceClient,
            transactionRefundRepository,
            paymentGatewayClient,
            refundRetryService
        )

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
                notificationRequested
            )
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
                BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
            )
        )
            .expectNext()
            .verifyComplete()
        verify(checkpointer, times(1)).success()
        verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
        verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
        verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
        verify(transactionsViewRepository, times(1)).save(any())
        verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
        verify(transactionRefundRepository, times(0)).save(any())
        verify(paymentGatewayClient, times(0)).requestRefund(any())
        verify(transactionUserReceiptRepository, times(1)).save(any())
        verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
        assertEquals(TransactionStatusDto.NOTIFIED_OK, transactionViewRepositoryCaptor.value.status)
        val savedEvent = transactionUserReceiptCaptor.value
        assertEquals(TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT, savedEvent.eventCode)
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
                    notificationRequested
                )
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
            given(transactionRefundRepository.save(capture(transactionRefundEventStoreCaptor)))
                .willAnswer { Mono.just(it.arguments[0]) }
            given(paymentGatewayClient.requestRefund(any()))
                .willReturn(Mono.just(PostePayRefundResponseDto().refundOutcome("OK")))
            StepVerifier.create(
                transactionNotificationsRetryQueueConsumer.messageReceiver(
                    BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
                )
            )
                .expectNext()
                .verifyComplete()
            verify(checkpointer, times(1)).success()
            verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
            verify(transactionRefundRepository, times(2)).save(any())
            verify(paymentGatewayClient, times(1)).requestRefund(any())
            verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
            verify(transactionsViewRepository, times(3)).save(any())
            verify(transactionUserReceiptRepository, times(1)).save(any())
            verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
            verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
            verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
            val expectedStatuses =
                listOf(
                    TransactionStatusDto.NOTIFIED_KO,
                    TransactionStatusDto.REFUND_REQUESTED,
                    TransactionStatusDto.REFUNDED
                )
            val expectedEventCodes =
                listOf(
                    TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
                    TransactionEventCode.TRANSACTION_REFUNDED_EVENT
                )
            assertEquals(
                TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
                transactionUserReceiptCaptor.value.eventCode
            )
            assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
            expectedEventCodes.forEachIndexed { index, eventCode ->
                assertEquals(eventCode, transactionRefundEventStoreCaptor.allValues[index].eventCode)
                assertEquals(
                    TransactionStatusDto.NOTIFICATION_REQUESTED,
                    transactionRefundEventStoreCaptor.allValues[index].data.statusBeforeRefunded
                )
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
                    notificationRequested
                )
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
            given(transactionRefundRepository.save(capture(transactionRefundEventStoreCaptor)))
                .willAnswer { Mono.just(it.arguments[0]) }
            given(paymentGatewayClient.requestRefund(any()))
                .willReturn(Mono.error(RuntimeException("Error performing refunding")))
            given(refundRetryService.enqueueRetryEvent(any(), any())).willReturn(Mono.empty())
            StepVerifier.create(
                transactionNotificationsRetryQueueConsumer.messageReceiver(
                    BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
                )
            )
                .expectNext()
                .verifyComplete()
            verify(checkpointer, times(1)).success()
            verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
            verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
            verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
            verify(transactionRefundRepository, times(2)).save(any())
            verify(paymentGatewayClient, times(1)).requestRefund(any())
            verify(transactionsViewRepository, times(3)).save(any())
            verify(transactionUserReceiptRepository, times(1)).save(any())
            verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any())
            verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
            val expectedStatuses =
                listOf(
                    TransactionStatusDto.NOTIFIED_KO,
                    TransactionStatusDto.REFUND_REQUESTED,
                    TransactionStatusDto.REFUND_ERROR
                )
            val expectedEventCodes =
                listOf(
                    TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
                    TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT
                )
            assertEquals(
                TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT,
                transactionUserReceiptCaptor.value.eventCode
            )
            assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
            expectedEventCodes.forEachIndexed { index, eventCode ->
                assertEquals(eventCode, transactionRefundEventStoreCaptor.allValues[index].eventCode)
                assertEquals(
                    TransactionStatusDto.NOTIFICATION_REQUESTED,
                    transactionRefundEventStoreCaptor.allValues[index].data.statusBeforeRefunded
                )
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
                    notificationRequested
                )
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
                    BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
                )
            )
                .expectNext()
                .verifyComplete()
            verify(checkpointer, times(1)).success()
            verify(transactionsEventStoreRepository, times(1)).findByTransactionId(transactionId)
            verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
            verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
            verify(transactionsViewRepository, times(1)).save(any())
            verify(transactionRefundRepository, times(0)).save(any())
            verify(paymentGatewayClient, times(0)).requestRefund(any())
            verify(transactionUserReceiptRepository, times(1)).save(any())
            verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
            verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)

            assertEquals(0, retryCountCaptor.value)
            assertEquals(
                TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT,
                transactionUserReceiptCaptor.value.eventCode
            )
            assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.value.data)
            assertEquals(TransactionStatusDto.NOTIFICATION_ERROR, transactionViewRepositoryCaptor.value.status)

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
                    notificationRequested
                )
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
                    BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
                )
            )
                .expectNext()
                .verifyComplete()
            verify(checkpointer, times(1)).success()
            verify(transactionsEventStoreRepository, times(1)).findByTransactionId(transactionId)
            verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
            verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
            verify(transactionsViewRepository, times(1)).save(any())
            verify(transactionRefundRepository, times(0)).save(any())
            verify(paymentGatewayClient, times(0)).requestRefund(any())
            verify(transactionUserReceiptRepository, times(1)).save(any())
            verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
            verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
            assertEquals(0, retryCountCaptor.value)
            val expectedStatuses =
                listOf(
                    TransactionStatusDto.NOTIFICATION_ERROR,
                )
            val expectedEventCodes =
                listOf(
                    TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT
                )
            expectedEventCodes.forEachIndexed { index, eventCode ->
                assertEquals(eventCode, transactionUserReceiptCaptor.allValues[index].eventCode)
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
                    notificationRequested
                )
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
                    BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
                )
            )
                .expectError(RuntimeException::class.java)
                .verify()
            verify(checkpointer, times(1)).success()
            verify(transactionsEventStoreRepository, times(1)).findByTransactionId(transactionId)
            verify(notificationsServiceClient, times(1)).sendNotificationEmail(any())
            verify(notificationRetryService, times(1)).enqueueRetryEvent(any(), any())
            verify(transactionsViewRepository, times(1)).save(any())
            verify(transactionRefundRepository, times(0)).save(any())
            verify(paymentGatewayClient, times(0)).requestRefund(any())
            verify(transactionUserReceiptRepository, times(1)).save(any())
            verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
            verify(userReceiptMailBuilder, times(1)).buildNotificationEmailRequestDto(baseTransaction)
            assertEquals(0, retryCountCaptor.value)
            val expectedStatuses =
                listOf(
                    TransactionStatusDto.NOTIFICATION_ERROR,
                )
            val expectedEventCodes =
                listOf(
                    TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT
                )
            expectedEventCodes.forEachIndexed { index, eventCode ->
                assertEquals(eventCode, transactionUserReceiptCaptor.allValues[index].eventCode)
                assertEquals(transactionUserReceiptData, transactionUserReceiptCaptor.allValues[index].data)

            }
            expectedStatuses.forEachIndexed { index, transactionStatus ->
                assertEquals(transactionStatus, transactionViewRepositoryCaptor.allValues[index].status)
            }
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
                BinaryData.fromObject(notificationRequested).toBytes(), checkpointer
            )
        )
            .expectError(BadTransactionStatusException::class.java)
            .verify()
        verify(checkpointer, times(1)).success()
        verify(transactionsEventStoreRepository, times(1)).findByTransactionId(any())
        verify(notificationsServiceClient, times(0)).sendNotificationEmail(any())
        verify(notificationRetryService, times(0)).enqueueRetryEvent(any(), any())
        verify(transactionsViewRepository, times(0)).save(any())
        verify(transactionRefundRepository, times(0)).save(any())
        verify(paymentGatewayClient, times(0)).requestRefund(any())
        verify(transactionUserReceiptRepository, times(0)).save(any())
        verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
        verify(userReceiptMailBuilder, times(0)).buildNotificationEmailRequestDto(any())
    }
}
