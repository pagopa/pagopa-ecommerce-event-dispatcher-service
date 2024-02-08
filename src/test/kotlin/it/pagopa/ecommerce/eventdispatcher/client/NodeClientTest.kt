package it.pagopa.ecommerce.eventdispatcher.client

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedClosePaymentRequest
import it.pagopa.generated.ecommerce.nodo.v2.ApiClient
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
import java.nio.charset.Charset
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.BDDMockito.given
import org.mockito.Mock
import org.mockito.kotlin.mock
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.TestPropertySource
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@OptIn(ExperimentalCoroutinesApi::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class NodeClientTest {

  companion object {
    const val CLOSE_PAYMENT_CLIENT_ID = "ecomm"
  }

  @Mock private lateinit var nodeApi: NodoApi

  private val apiClient: ApiClient = mock {}

  private val objectMapper = ObjectMapper()

  private lateinit var nodeClient: NodeClient

  @BeforeEach
  fun init() {
    nodeClient = NodeClient(nodeApi, CLOSE_PAYMENT_CLIENT_ID)
  }

  @Test
  fun `closePayment returns successfully`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

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
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.apiClient).willReturn(apiClient)
    given(apiClient.objectMapper).willReturn(objectMapper)
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            404, "Not found", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    /* test */
    val exception =
      assertThrows<ClosePaymentErrorResponseException> {
        nodeClient.closePayment(closePaymentRequest).awaitSingle()
      }
    assertEquals(HttpStatus.NOT_FOUND, exception.statusCode)
    assertNull(exception.errorResponse)
  }

  @Test
  fun `closePayment throws GatewayTimeoutException on Node 408`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.apiClient).willReturn(apiClient)
    given(apiClient.objectMapper).willReturn(objectMapper)
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            408, "Request timeout", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    /* test */
    val exception =
      assertThrows<ClosePaymentErrorResponseException> {
        nodeClient.closePayment(closePaymentRequest).awaitSingle()
      }
    assertEquals(HttpStatus.REQUEST_TIMEOUT, exception.statusCode)
    assertNull(exception.errorResponse)
  }

  @Test
  fun `closePayment throws BadGatewayException on Node 500`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.apiClient).willReturn(apiClient)
    given(apiClient.objectMapper).willReturn(objectMapper)
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
    val exception =
      assertThrows<ClosePaymentErrorResponseException> {
        nodeClient.closePayment(closePaymentRequest).awaitSingle()
      }
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.statusCode)
    assertNull(exception.errorResponse)
  }

  @Test
  fun `closePayment throws BadClosePaymentRequest on Node 400`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest = getMockedClosePaymentRequest(transactionId, OutcomeEnum.OK)

    /* preconditions */
    given(nodeApi.closePaymentV2(closePaymentRequest, CLOSE_PAYMENT_CLIENT_ID))
      .willReturn(
        Mono.error(
          WebClientResponseException.create(
            400, "Bad request", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    /* test */

    val exception =
      assertThrows<ClosePaymentErrorResponseException> {
        nodeClient.closePayment(closePaymentRequest).awaitSingle()
      }
    assertEquals(HttpStatus.BAD_REQUEST, exception.statusCode)
    assertNull(exception.errorResponse)
  }

  @Test
  fun `Should extract error response information from Nodo error response`() = runTest {
    val mockServer = MockWebServer()
    val expectedNodeErrorDescription = "NODE ERROR DESCRIPTION"
    val nodeErrorResponse = ErrorDto().outcome("KO").description(expectedNodeErrorDescription)
    mockServer.use {
      // pre-requisites
      mockServer.start(8080)
      mockServer.enqueue(
        MockResponse()
          .setStatus("ERROR")
          .setResponseCode(400)
          .setBody(objectMapper.writeValueAsString(nodeErrorResponse)))

      val nodeClient =
        NodeClient(NodoApi(ApiClient().setBasePath("http://localhost:8080")), "clientId")
      // test
      StepVerifier.create(nodeClient.closePayment(ClosePaymentRequestV2Dto()))
        .expectErrorMatches {
          assertTrue(it is ClosePaymentErrorResponseException)
          assertEquals(
            expectedNodeErrorDescription,
            (it as ClosePaymentErrorResponseException).errorResponse!!.description)
          assertEquals(HttpStatus.BAD_REQUEST, it.statusCode)
          true
        }
        .verify()
    }
  }
}
