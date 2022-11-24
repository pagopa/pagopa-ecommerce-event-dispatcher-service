package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.exceptions.BadGatewayException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.transactions.server.model.AuthorizationResultDto
import it.pagopa.generated.transactions.server.model.TransactionStatusDto
import it.pagopa.transactions.documents.*
import it.pagopa.transactions.utils.TransactionEventCode
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.beans.factory.annotation.Autowired
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
                "authorizationRequestId",
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

        val transaction = Transaction(
            transactionId,
            paymentToken,
            rptId,
            "description",
            1200,
            "email@test.it",
            TransactionStatusDto.EXPIRED
        )

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
        given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))

        /* test */
        transactionActivatedEventsConsumer.messageReceiver(
            BinaryData.fromObject(activatedEvent).toBytes(),
            checkpointer
        )

        /* Asserts */
        verify(checkpointer, times(1)).success()
    }
/*
    @Test
    fun `messageReceiver handles exceptions on closePayment failure`() = runTest {
        val transactionId = UUID.randomUUID()
        val rptId = "rptId"
        val paymentToken = "paymentToken"

        val authorizationRequestedEvent = TransactionAuthorizationRequestedEvent(
            transactionId.toString(),
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
                "authorizationRequestId"
            )
        )

        val postePayRefundResponse = PostePayRefundResponseDto().apply {
            requestId = ""
            paymentId = ""
            refundOutcome = ""
            error = ""
        }

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(
                transactionId.toString(),
                TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT
            )
        )
            .willReturn(Mono.empty())
        given(refundService.requestRefund(any())).willReturn(Mono.just(postePayRefundResponse))
        given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
            .willThrow(BadGatewayException(""))

        /* test */
        assertDoesNotThrow {
            transactionAuthRequestedEventsConsumer.messageReceiver(
                BinaryData.fromObject(authorizationRequestedEvent).toBytes(),
                checkpointer
            )
        }

        /* Asserts */
        verify(checkpointer, times(1)).success()
        verify(refundService, times(1)).requestRefund(any())
        verify(nodeService, times(1)).closePayment(any(), any())
    }

    @Test
    fun `messageReceiver handles exceptions on requestRefund failure`() = runTest {
        val transactionId = UUID.randomUUID()
        val rptId = "rptId"
        val paymentToken = "paymentToken"

        val authorizationRequestedEvent = TransactionAuthorizationRequestedEvent(
            transactionId.toString(),
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
                "authorizationRequestId"
            )
        )

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(
            transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(
                transactionId.toString(),
                TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT
            )
        )
            .willReturn(Mono.empty())
        given(refundService.requestRefund(any())).willReturn(Mono.error(BadGatewayException("")))
        given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
            .willReturn(ClosePaymentResponseDto().esito(ClosePaymentResponseDto.EsitoEnum.KO))

        /* test */
        assertDoesNotThrow {
            transactionAuthRequestedEventsConsumer.messageReceiver(
                BinaryData.fromObject(authorizationRequestedEvent).toBytes(),
                checkpointer
            )
        }

        /* Asserts */
        verify(checkpointer, times(1)).success()
        verify(refundService, times(1)).requestRefund(any())
        verify(nodeService, times(1)).closePayment(any(), any())
    }
     */
}
