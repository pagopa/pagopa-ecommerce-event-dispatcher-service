package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.scheduler.exceptions.BadGatewayException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentResponseDto
import it.pagopa.generated.transactions.server.model.AuthorizationResultDto
import it.pagopa.generated.transactions.server.model.TransactionStatusDto
import it.pagopa.transactions.documents.TransactionAuthorizationRequestData
import it.pagopa.transactions.documents.TransactionAuthorizationRequestedEvent
import it.pagopa.transactions.documents.TransactionAuthorizationStatusUpdateData
import it.pagopa.transactions.documents.TransactionAuthorizationStatusUpdatedEvent
import it.pagopa.transactions.utils.TransactionEventCode
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono
import java.util.*

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionAuthRequestedEventsConsumerTests {

    @Mock private lateinit var checkpointer: Checkpointer

    @Mock
    private lateinit var nodeService: NodeService

    @Mock
    private lateinit var refundService: RefundService

    @Mock
    private lateinit var transactionsAuthorizationStatusUpdateEventRepository: TransactionsEventStoreRepository<TransactionAuthorizationStatusUpdateData>

    @InjectMocks
    private lateinit var transactionAuthRequestedEventsConsumer:
            TransactionAuthRequestedEventsConsumer

    @Test
    fun `messageReceiver receives messages successfully`() {
        val transactionId = "transactionId"
        val rptId = "rptId"
        val paymentToken = "paymentToken"

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
                "requestId"
            )
        )

        val statusUpdateEvent = TransactionAuthorizationStatusUpdatedEvent(
            transactionId,
            rptId,
            paymentToken,
            TransactionAuthorizationStatusUpdateData(AuthorizationResultDto.OK, TransactionStatusDto.AUTHORIZED)
        )

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(transactionId, TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT))
            .willReturn(Mono.just(statusUpdateEvent))

        /* test */
        transactionAuthRequestedEventsConsumer.messageReceiver(
                BinaryData.fromObject(authorizationRequestedEvent).toBytes(),
                checkpointer
        )

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
    }

    @Test
    fun `messageReceiver calls refund and closepayment on non-existing authorization status update event`() = runTest {
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
                "requestId"
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
        given(transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(transactionId.toString(), TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT))
            .willReturn(Mono.empty())
        given(refundService.requestRefund(any())).willReturn(Mono.just(postePayRefundResponse))
        given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.OK))
            .willReturn(ClosePaymentResponseDto().esito(ClosePaymentResponseDto.EsitoEnum.KO))

        /* test */
        transactionAuthRequestedEventsConsumer.messageReceiver(
            BinaryData.fromObject(authorizationRequestedEvent).toBytes(),
            checkpointer
        )

        /* Asserts */
        verify(checkpointer, times(1)).success()
        verify(refundService, times(1)).requestRefund(any())
        verify(nodeService, times(1)).closePayment(any(), any())
    }

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
                "requestId"
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
        given(transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(transactionId.toString(), TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT))
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
                "requestId"
            )
        )

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(transactionId.toString(), TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT))
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
}
