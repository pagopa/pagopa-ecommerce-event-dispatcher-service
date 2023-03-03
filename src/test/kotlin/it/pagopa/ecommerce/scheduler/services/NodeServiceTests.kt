package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.commons.documents.*
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.transactionActivateEvent
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.transactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.scheduler.client.NodeClient
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
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

@ExtendWith(SpringExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class NodeServiceTests {

  @InjectMocks lateinit var nodeService: NodeService

  @Mock lateinit var nodeClient: NodeClient

  @Mock lateinit var transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>

  @Test
  fun `closePayment returns successfully`() = runTest {
    val transactionOutcome = OutcomeEnum.OK

    val activatedEvent = transactionActivateEvent()
    val authEvent = transactionAuthorizationRequestedEvent()

    val transactionId = activatedEvent.transactionId

    val closePaymentResponse =
      ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

    /* preconditions */
    given(
        transactionsEventStoreRepository.findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
      .willReturn(Mono.just(activatedEvent as TransactionEvent<Any>))

    given(
        transactionsEventStoreRepository.findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
      .willReturn(Mono.just(authEvent as TransactionEvent<Any>))

    given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

    /* test */
    assertEquals(
      closePaymentResponse,
      nodeService.closePayment(UUID.fromString(transactionId), transactionOutcome))
  }

  @Test
  fun `closePayment throws TransactionEventNotFoundException on transaction event not found`() =
    runTest {
      val transactionId = UUID.randomUUID()
      val transactionOutcome = OutcomeEnum.OK

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
        .willReturn(Mono.empty())

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
        .willReturn(Mono.empty())

      /* test */

      assertThrows<TransactionEventNotFoundException> {
        nodeService.closePayment(transactionId, transactionOutcome)
      }
    }
}
