package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TRANSIENT_QUEUE_TTL_SECONDS
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.messaging.MessageHeaders
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration
import java.time.ZonedDateTime

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionExpirationQueueConsumerTests {

    private val checkpointer: Checkpointer = mock()

    private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

    private val paymentGatewayClient: PaymentGatewayClient = mock()

    private val transactionsExpiredEventStoreRepository:
            TransactionsEventStoreRepository<TransactionExpiredData> =
        mock()

    private val transactionsRefundedEventStoreRepository:
            TransactionsEventStoreRepository<TransactionRefundedData> =
        mock()

    private val transactionsViewRepository: TransactionsViewRepository = mock()

    private val refundRetryService: RefundRetryService = mock()

    @Captor
    private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

    @Captor
    private lateinit var transactionRefundEventStoreCaptor:
            ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

    @Captor
    private lateinit var transactionExpiredEventStoreCaptor:
            ArgumentCaptor<TransactionEvent<TransactionExpiredData>>

    @Captor
    private lateinit var retryCountCaptor: ArgumentCaptor<Int>

    @Captor
    private lateinit var queueEventCaptor: ArgumentCaptor<BinaryData>

    @Captor
    private lateinit var binaryDataCaptor: ArgumentCaptor<BinaryData>

    @Captor
    private lateinit var visibilityTimeoutCaptor: ArgumentCaptor<Duration>

    private val transactionUtils = TransactionUtils()

    private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

    private val expirationQueueAsyncClient: QueueAsyncClient = mock()

    private val sendPaymentResultTimeout = 120

    private val sendPaymentResultOffset = 10

    private val tracingUtils = TracingUtilsTests.getMock()

    private val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
            paymentGatewayClient = paymentGatewayClient,
            transactionsEventStoreRepository = transactionsEventStoreRepository,
            transactionsExpiredEventStoreRepository = transactionsExpiredEventStoreRepository,
            transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
            transactionsViewRepository = transactionsViewRepository,
            transactionUtils = transactionUtils,
            refundRetryService = refundRetryService,
            deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
            expirationQueueAsyncClient = expirationQueueAsyncClient,
            sendPaymentResultTimeoutSeconds = sendPaymentResultTimeout,
            sendPaymentResultTimeoutOffsetSeconds = sendPaymentResultOffset,
            transientQueueTTLSeconds = TRANSIENT_QUEUE_TTL_SECONDS,
            tracingUtils = tracingUtils
        )

    @Test
    fun `messageReceiver receives activated messages successfully`() {
        val activatedEvent = transactionActivateEvent()
        val transactionId = activatedEvent.transactionId

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
                transactionId,
            )
        )
            .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
        given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
        given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
            Mono.just(it.arguments[0])
        }

        given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
            .willReturn(
                Mono.just(
                    transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
                )
            )

        /* test */
        StepVerifier.create(
            transactionExpirationQueueConsumer.messageReceiver(
                Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
                        MOCK_TRACING_INFO,
                checkpointer,
                MessageHeaders(mapOf())
            )
        )
            .expectNext(Unit)
            .expectComplete()
            .verify()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
    }

    @Test
    fun `messageReceiver receives legacy activated messages successfully`() {
        val activatedEvent = transactionActivateEvent()
        val transactionId = activatedEvent.transactionId

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
                transactionId,
            )
        )
            .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
        given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
        given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
            Mono.just(it.arguments[0])
        }

        given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
            .willReturn(
                Mono.just(
                    transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())
                )
            )

        /* test */
        StepVerifier.create(
            transactionExpirationQueueConsumer.messageReceiver(
                Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to null,
                checkpointer,
                MessageHeaders(mapOf())
            )
        )
            .expectNext(Unit)
            .expectComplete()
            .verify()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
    }

    @Test
    fun `messageReceiver receives legacy expiration messages successfully`() {
        val activatedEvent = transactionActivateEvent()
        val tx = reduceEvents(activatedEvent)

        val expiredEvent = transactionExpiredEvent(tx)

        val transactionId = activatedEvent.transactionId

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
                transactionId,
            )
        )
            .willReturn(
                Flux.just(activatedEvent as TransactionEvent<Any>, expiredEvent as TransactionEvent<Any>)
            )
        given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
        given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
            Mono.just(it.arguments[0])
        }

        given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
            .willReturn(
                Mono.just(
                    transactionDocument(TransactionStatusDto.EXPIRED_NOT_AUTHORIZED, ZonedDateTime.now())
                )
            )

        /* test */
        StepVerifier.create(
            transactionExpirationQueueConsumer.messageReceiver(
                Either.right<TransactionActivatedEvent, TransactionExpiredEvent>(expiredEvent) to null,
                checkpointer,
                MessageHeaders(mapOf())
            )
        )
            .expectNext(Unit)
            .expectComplete()
            .verify()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
    }

    @Test
    fun `messageReceiver forward event into dead letter queue for exception processing the event`() =
        runTest {
            /* preconditions */

            val activatedEvent = transactionActivateEvent()

            /* preconditions */
            given(checkpointer.success()).willReturn(Mono.empty())
            given(
                transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
                    any(),
                )
            )
                .willReturn(Flux.error(RuntimeException("Error finding event from event store")))
            given(
                deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
                    any<BinaryData>(), any()
                )
            )
                .willReturn(mono {})

            /* test */
            StepVerifier.create(
                transactionExpirationQueueConsumer.messageReceiver(
                    Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
                            MOCK_TRACING_INFO,
                    checkpointer,
                    MessageHeaders(mapOf())
                )
            )
                .expectNext(Unit)
                .verifyComplete()

            /* Asserts */
            verify(checkpointer, times(1)).success()
            verify(deadLetterTracedQueueAsyncClient, times(1))
                .sendAndTraceDeadLetterQueueEvent(
                    argThat<BinaryData> {
                        TransactionEventCode.valueOf(
                            this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                                .event
                                .eventCode
                        ) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
                    },
                    eq(
                        DeadLetterTracedQueueAsyncClient.ErrorContext(
                            transactionId = TransactionId(TRANSACTION_ID),
                            transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
                            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR
                        )
                    )
                )
        }

    @Test
    fun `messageReceiver processing should fail for error forward event into dead letter queue for exception processing the event`() =
        runTest {
            /* preconditions */

            val activatedEvent = transactionActivateEvent()

            /* preconditions */
            given(checkpointer.success()).willReturn(Mono.empty())
            given(
                transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
                    any(),
                )
            )
                .willReturn(Flux.error(RuntimeException("Error finding event from event store")))
            given(
                deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
                    any<BinaryData>(), any()
                )
            )
                .willReturn(Mono.error(RuntimeException("Error sending event to dead letter queue")))

            /* test */
            StepVerifier.create(
                transactionExpirationQueueConsumer.messageReceiver(
                    Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
                            MOCK_TRACING_INFO,
                    checkpointer,
                    MessageHeaders(mapOf())
                )
            )
                .expectErrorMatches { it.message == "Error sending event to dead letter queue" }
                .verify()

            /* Asserts */
            verify(checkpointer, times(1)).success()
            verify(deadLetterTracedQueueAsyncClient, times(1))
                .sendAndTraceDeadLetterQueueEvent(
                    argThat<BinaryData> {
                        TransactionEventCode.valueOf(
                            this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                                .event
                                .eventCode
                        ) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
                    },
                    eq(
                        DeadLetterTracedQueueAsyncClient.ErrorContext(
                            transactionId = TransactionId(TRANSACTION_ID),
                            transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
                            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR
                        )
                    )
                )
        }

}
