package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.BadClosePaymentRequest
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedClosePaymentRequest
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.nio.charset.Charset
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.BDDMockito.given
import org.mockito.InjectMocks
import org.mockito.Mock
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.TestPropertySource
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@SpringBootTest
@OptIn(ExperimentalCoroutinesApi::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class NodeClientTest {

  @Mock private lateinit var nodeApi: NodoApi

  @InjectMocks private lateinit var nodeClient: NodeClient
  companion object {
    const val CLOSE_PAYMENT_CLIENT_ID: String = "ecomm"
  }
  @Test
  fun `closePayment returns successfully`() = runTest {
    val transactionId: UUID = UUID.randomUUID()

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)
    val expected =
      ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

    /* preconditions */
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(Mono.just(expected))

    /* test */
    val response = nodeClient.closePayment(closePaymentRequest).awaitSingle()

    assertEquals(expected, response)
  }

  @Test
  fun `closePayment throws TransactionEventNotFoundException on Node 404`() = runTest {
    val transactionId: UUID = UUID.randomUUID()

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            404, "Not found", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    /* test */
    assertThrows<TransactionNotFound> { nodeClient.closePayment(closePaymentRequest).awaitSingle() }
  }

  @Test
  fun `closePayment throws GatewayTimeoutException on Node 408`() = runTest {
    val transactionId: UUID = UUID.randomUUID()

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            408, "Request timeout", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    /* test */
    assertThrows<GatewayTimeoutException> {
      nodeClient.closePayment(closePaymentRequest).awaitSingle()
    }
  }

  @Test
  fun `closePayment throws BadGatewayException on Node 500`() = runTest {
    val transactionId: UUID = UUID.randomUUID()

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            500,
            "Internal server error",
            HttpHeaders.EMPTY,
            ByteArray(0),
            Charset.defaultCharset())))

    /* test */
    assertThrows<BadGatewayException> { nodeClient.closePayment(closePaymentRequest).awaitSingle() }
  }

  @Test
  fun `closePayment throws BadClosePaymentRequest on Node 400`() = runTest {
    val transactionId: UUID = UUID.randomUUID()

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            400, "Bad request", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    /* test */
    assertThrows<BadClosePaymentRequest> {
      nodeClient.closePayment(closePaymentRequest).awaitSingle()
    }
  }
}
