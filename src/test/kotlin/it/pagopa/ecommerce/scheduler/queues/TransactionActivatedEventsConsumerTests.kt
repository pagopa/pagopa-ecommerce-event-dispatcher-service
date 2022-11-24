package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.transactions.server.model.TransactionStatusDto
import it.pagopa.transactions.documents.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.*

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionActivatedEventsConsumerTests {

    @Mock
    private lateinit var checkpointer: Checkpointer

    @Mock
    private lateinit var nodeService: NodeService

    @Mock
    private lateinit var refundService: RefundService

    @Mock
    private lateinit var transactionsEventStoreRepository: TransactionsEventStoreRepository<Objects>

    @Mock
    private lateinit var paymentGatewayClient: PaymentGatewayClient

    @Mock
    private lateinit var transactionsExpiredEventStoreRepository: TransactionsEventStoreRepository<TransactionExpiredData>

    @Mock
    private lateinit var transactionsRefundedEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>

    @Mock
    private lateinit var transactionsViewRepository: TransactionsViewRepository


    @Test
    fun `messageReceiver receives messages successfully`() {
        var transactionActivatedEventsConsumer:
                TransactionActivatedEventsConsumer =
            TransactionActivatedEventsConsumer(
                paymentGatewayClient,
                transactionsEventStoreRepository,
                transactionsExpiredEventStoreRepository,
                transactionsRefundedEventStoreRepository,
                transactionsViewRepository
            )

        val transactionId = UUID.randomUUID().toString()
        val rptId = "77777777777302000100440009424"
        val paymentToken = UUID.randomUUID().toString().replace("-", "")

        val activatedEvent = TransactionActivatedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionActivatedData(
                "description",
                100,
                "email@test.it",
                null,
                null,
                paymentToken,
            )
        )


        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsEventStoreRepository.findByTransactionId(
                transactionId,
            )
        )
            .willReturn(Flux.just(activatedEvent as TransactionEvent<Objects>))

        /* test */
        transactionActivatedEventsConsumer.messageReceiver(
            BinaryData.fromObject(activatedEvent).toBytes(),
            checkpointer
        )

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
    }


    @Test
    fun `messageReceiver calls refund on transaction with authorization request`() = runTest {
        var transactionActivatedEventsConsumer:
                TransactionActivatedEventsConsumer =
            TransactionActivatedEventsConsumer(
                paymentGatewayClient,
                transactionsEventStoreRepository,
                transactionsExpiredEventStoreRepository,
                transactionsRefundedEventStoreRepository,
                transactionsViewRepository
            )
        val transactionId = UUID.randomUUID().toString()
        val rptId = "77777777777302000100440009424"
        val paymentToken = UUID.randomUUID().toString().replace("-", "")

        val activatedEvent = TransactionActivatedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionActivatedData(
                "description",
                100,
                "email@test.it",
                null,
                null,
                paymentToken,
            )
        )

        val authorizationRequestedEvent = TransactionAuthorizationRequestedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionAuthorizationRequestData(
                100,
                0,
                "paymentInstrumentId",
                "pspId",
                "paymentTypeCode",
                "brokerName",
                "pspChannelCode",
                "requestId",
                "pspBusinessName",
                UUID.randomUUID().toString(),
            )
        )

        val expiredEvent = TransactionExpiredEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionExpiredData(
                TransactionStatusDto.ACTIVATED
            )
        )

        val refundedEvent = TransactionRefundedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionRefundedData(
                TransactionStatusDto.ACTIVATED
            )
        )

        val transaction = Transaction(
            transactionId,
            paymentToken,
            rptId,
            "description",
            1200,
            "email@test.it",
            TransactionStatusDto.EXPIRED
        )

        val gatewayClientResponse = PostePayRefundResponseDto()
        gatewayClientResponse.refundOutcome = "OK"

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsEventStoreRepository.findByTransactionId(
                any(),
            )
        )
            .willReturn(
                Flux.just(
                    activatedEvent as TransactionEvent<Objects>,
                    authorizationRequestedEvent as TransactionEvent<Objects>
                )
            )

        given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
        given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
        given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
        given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

        /* test */
        transactionActivatedEventsConsumer.messageReceiver(
            BinaryData.fromObject(activatedEvent).toBytes(),
            checkpointer
        )

        /* Asserts */
        verify(checkpointer, times(1)).success()
        verify(paymentGatewayClient, times(1)).requestRefund(any())
    }

    @Test
    fun `messageReceiver calls refund and handle error with retry`() = runTest {
        var transactionActivatedEventsConsumer:
                TransactionActivatedEventsConsumer =
            TransactionActivatedEventsConsumer(
                paymentGatewayClient,
                transactionsEventStoreRepository,
                transactionsExpiredEventStoreRepository,
                transactionsRefundedEventStoreRepository,
                transactionsViewRepository
            )
        val transactionId = UUID.randomUUID().toString()
        val rptId = "77777777777302000100440009424"
        val paymentToken = UUID.randomUUID().toString().replace("-", "")

        val activatedEvent = TransactionActivatedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionActivatedData(
                "description",
                100,
                "email@test.it",
                null,
                null,
                paymentToken,
            )
        )

        val authorizationRequestedEvent = TransactionAuthorizationRequestedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionAuthorizationRequestData(
                100,
                0,
                "paymentInstrumentId",
                "pspId",
                "paymentTypeCode",
                "brokerName",
                "pspChannelCode",
                "requestId",
                "pspBusinessName",
                UUID.randomUUID().toString(),
            )
        )

        val expiredEvent = TransactionExpiredEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionExpiredData(
                TransactionStatusDto.ACTIVATED
            )
        )

        val refundedEvent = TransactionRefundedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionRefundedData(
                TransactionStatusDto.ACTIVATED
            )
        )

        val transaction = Transaction(
            transactionId,
            paymentToken,
            rptId,
            "description",
            1200,
            "email@test.it",
            TransactionStatusDto.EXPIRED
        )

        val gatewayClientResponse = PostePayRefundResponseDto()
        gatewayClientResponse.refundOutcome = "KO"

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsEventStoreRepository.findByTransactionId(
                any(),
            )
        )
            .willReturn(
                Flux.just(
                    activatedEvent as TransactionEvent<Objects>,
                    authorizationRequestedEvent as TransactionEvent<Objects>
                )
            )

        given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
        given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
        given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
        given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

        /* test */
        transactionActivatedEventsConsumer.messageReceiver(
            BinaryData.fromObject(activatedEvent).toBytes(),
            checkpointer
        )

        /* Asserts */
        verify(checkpointer, times(1)).success()
        verify(paymentGatewayClient, times(1)).requestRefund(any())
    }
}
