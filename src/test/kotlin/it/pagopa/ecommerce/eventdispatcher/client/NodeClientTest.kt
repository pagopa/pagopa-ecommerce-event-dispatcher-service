package it.pagopa.ecommerce.eventdispatcher.client

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.config.WebClientConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedCardClosePaymentRequest
import it.pagopa.generated.ecommerce.nodo.v2.dto.CardClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpStatus
import org.springframework.test.context.TestPropertySource
import reactor.test.StepVerifier

@SpringBootTest
@OptIn(ExperimentalCoroutinesApi::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class NodeClientTest {

  companion object {

    val mockWebServer = MockWebServer()

    @JvmStatic
    @BeforeAll
    fun beforeAllTest() {
      mockWebServer.start(8080)
      println("Mock web server listening on ${mockWebServer.hostName}:${mockWebServer.port}")
    }

    @JvmStatic
    @AfterAll
    fun afterAllTest() {
      mockWebServer.shutdown()
      println("Mock web server stop")
    }
  }

  val nodeClient =
    NodeClient(
      WebClientConfig()
        .nodoApi(
          nodoUri = "http://localhost:8080",
          nodoConnectionTimeout = 1000,
          nodoReadTimeout = 1000,
          nodeForEcommerceApiKey = "nodeForEcommerceApiKey"),
      "ecomm",
      ObjectMapper())

  private val closePaymentRequest =
    CardClosePaymentRequestV2Dto()
      .transactionId(TransactionTestUtils.TRANSACTION_ID)
      .paymentTokens(listOf(TransactionTestUtils.PAYMENT_TOKEN))
      .outcome(CardClosePaymentRequestV2Dto.OutcomeEnum.OK)

  @Test
  fun `closePayment returns successfully`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest =
      getMockedCardClosePaymentRequest(transactionId, ClosePaymentOutcome.OK)
    val expected =
      ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse()
                .setStatus("OK")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(
                  """
                            {
                                "outcome": "OK"
                            }
                        """.trimIndent())
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher
    /* test */
    val response = nodeClient.closePayment(closePaymentRequest).awaitSingle()

    assertEquals(expected, response)

    // validate presence of your header
    val recordedRequest = mockWebServer.takeRequest()
    assertEquals("nodeForEcommerceApiKey", recordedRequest.getHeader("ocp-apim-subscription-key"))
  }

  @Test
  fun `closePayment throws TransactionEventNotFoundException on Node 404`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest =
      getMockedCardClosePaymentRequest(transactionId, ClosePaymentOutcome.OK)

    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse()
                .setStatus("NOT FOUND")
                .setResponseCode(404)
                .addHeader("Content-Type", "application/json")
                .setBody(
                  """
                            {
                                "outcome": "KO",
                                "description": "NOT FOUND"
                            }
                        """.trimIndent())
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher

    /* test */
    StepVerifier.create(nodeClient.closePayment(closePaymentRequest))
      .expectErrorMatches {
        assertTrue(it is ClosePaymentErrorResponseException)
        assertEquals(
          "NOT FOUND", (it as ClosePaymentErrorResponseException).errorResponse!!.description)
        assertEquals(HttpStatus.NOT_FOUND, it.statusCode)
        true
      }
      .verify()
  }

  @Test
  fun `closePayment handle error on Node 500`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest =
      getMockedCardClosePaymentRequest(transactionId, ClosePaymentOutcome.OK)

    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse()
                .setStatus("Internal server error")
                .setResponseCode(500)
                .addHeader("Content-Type", "application/json")
                .setBody(
                  """
                            {
                                "outcome": "KO",
                                "description": "Internal server error"
                            }
                        """.trimIndent())
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher
    /* test */
    StepVerifier.create(nodeClient.closePayment(closePaymentRequest))
      .expectErrorMatches {
        assertTrue(it is ClosePaymentErrorResponseException)
        assertEquals(
          "Internal server error",
          (it as ClosePaymentErrorResponseException).errorResponse!!.description)
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, it.statusCode)
        true
      }
      .verify()
  }

  @Test
  fun `closePayment handle Node 400`() = runTest {
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    val closePaymentRequest =
      getMockedCardClosePaymentRequest(transactionId, ClosePaymentOutcome.OK)

    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse()
                .setStatus("Bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
                .setBody(
                  """
                            {
                                "outcome": "KO",
                                "description": "Bad request"
                            }
                        """.trimIndent())
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher
    /* test */

    StepVerifier.create(nodeClient.closePayment(closePaymentRequest))
      .expectErrorMatches {
        assertTrue(it is ClosePaymentErrorResponseException)
        assertEquals(
          "Bad request", (it as ClosePaymentErrorResponseException).errorResponse!!.description)
        assertEquals(HttpStatus.BAD_REQUEST, it.statusCode)
        true
      }
      .verify()
  }

  @Test
  fun `Should extract error response information from Nodo error response`() = runTest {
    val expectedNodeErrorDescription = "NODE ERROR DESCRIPTION"
    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse()
                .setStatus("Bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
                .setBody(
                  """
                            {
                                "outcome": "KO",
                                "description": "$expectedNodeErrorDescription"
                            }
                        """.trimIndent())
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher

    // test
    StepVerifier.create(nodeClient.closePayment(closePaymentRequest))
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

  @Test
  fun `Should handle connection timeout`() = runTest {

    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher
    // test
    StepVerifier.create(nodeClient.closePayment(closePaymentRequest))
      .expectErrorMatches {
        assertTrue(it is ClosePaymentErrorResponseException)
        assertNull((it as ClosePaymentErrorResponseException).errorResponse)
        assertNull((it).statusCode)
        true
      }
      .verify()
  }

  @Test
  fun `Should handle invalid Nodo response body`() = runTest {

    // pre-requisites
    /* preconditions */
    val dispatcher: Dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
          return when (request.path) {
            "/closepayment?clientId=ecomm" ->
              return MockResponse()
                .setStatus("Bad request")
                .setResponseCode(400)
                .addHeader("Content-Type", "application/json")
                .setBody(
                  """
                            ERROR
                        """.trimIndent())
            else -> MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
          }
        }
      }
    mockWebServer.dispatcher = dispatcher

    // test
    StepVerifier.create(nodeClient.closePayment(closePaymentRequest))
      .expectErrorMatches {
        assertTrue(it is ClosePaymentErrorResponseException)
        assertNull((it as ClosePaymentErrorResponseException).errorResponse)
        assertEquals(HttpStatus.BAD_REQUEST, it.statusCode)
        true
      }
      .verify()
  }
}
