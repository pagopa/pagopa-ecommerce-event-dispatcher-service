package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.scheduler.client.NodeClient
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentResponseDto
import it.pagopa.transactions.documents.TransactionAuthorizationRequestData
import it.pagopa.transactions.documents.TransactionAuthorizationRequestedEvent
import it.pagopa.transactions.utils.TransactionEventCode
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.BDDMockito.given
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.kotlin.any
import org.springframework.test.context.junit.jupiter.SpringExtension
import reactor.core.publisher.Mono
import java.util.*

@ExtendWith(SpringExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class NodeServiceTests {

    @InjectMocks
    lateinit var nodeService: NodeService

    @Mock
    lateinit var nodeClient: NodeClient

    @Mock
    lateinit var transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionAuthorizationRequestData>

    @Test
    fun `closePayment returns successfully`() = runTest {
        val transactionId = UUID.randomUUID()
        val transactionOutcome = OutcomeEnum.OK

        val data = TransactionAuthorizationRequestData(
            100,
            1,
            "paymentInstrumentId",
            "pspId",
            "paymentTypeCode",
            "brokerName",
            "pspChannelCode",
            "requestId",
            "pspBusinessName",
            "authorizationRequestId"
            )

        val authEvent = TransactionAuthorizationRequestedEvent(transactionId.toString(), "", "", data)

        val closePaymentResponse = ClosePaymentResponseDto().apply {
            esito = ClosePaymentResponseDto.EsitoEnum.OK
        }

        /* preconditions */
        given(transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT
        )).willReturn(Mono.just(authEvent))

        given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

        /* test */
        assertEquals(closePaymentResponse, nodeService.closePayment(transactionId, transactionOutcome))
    }

    @Test
    fun `closePayment throws TransactionEventNotFoundException on transaction event not found`() = runTest {
        val transactionId = UUID.randomUUID()
        val transactionOutcome = OutcomeEnum.OK

        /* preconditions */
        given(transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT
        )).willReturn(Mono.empty())

        /* test */

        assertThrows<TransactionEventNotFoundException> {
            nodeService.closePayment(transactionId, transactionOutcome)
        }
    }
}