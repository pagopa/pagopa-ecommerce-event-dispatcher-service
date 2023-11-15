package it.pagopa.ecommerce.eventdispatcher.services

import com.fasterxml.jackson.databind.ObjectMapper
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.exceptions.NpgApiKeyMissingPspRequestedException
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.config.NpgPspsApiKeyConfigBuilder
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedVPosRefundRequest
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedXPayRefundRequest
import java.math.BigDecimal
import java.util.*
import java.util.stream.Stream
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpStatus
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class RefundServiceTests {
  private val paymentGatewayClient: PaymentGatewayClient = mock()
  private val tracer: Tracer = mock()
  private val span: Span = mock()
  private val spanBuilder: SpanBuilder = mock()
  private val paymentServiceApi =
    PaymentGatewayClient()
      .npgApiWebClient(
        npgClientUrl = "http://localhost:8080",
        npgWebClientConnectionTimeout = 10000,
        npgWebClientReadTimeout = 10000)
  private val npgClient: NpgClient = spy(NpgClient(paymentServiceApi, tracer, ObjectMapper()))
  private val npgPspApiKeys =
    NpgPspsApiKeyConfigBuilder()
      .npgCardsApiKeys("""
      {
        "pspId1": "pspKey1"
      }
    """, setOf("pspId1"))
  private val refundService: RefundService =
    RefundService(paymentGatewayClient, npgClient, npgPspApiKeys)

  companion object {
    lateinit var mockWebServer: MockWebServer

    @JvmStatic
    @BeforeAll
    fun setup() {
      mockWebServer = MockWebServer()
      mockWebServer.start(8080)
      println("Mock web server started on ${mockWebServer.hostName}:${mockWebServer.port}")
    }

    @JvmStatic
    @AfterAll
    fun tearDown() {
      mockWebServer.shutdown()
      println("Mock web stopped")
    }

    @JvmStatic
    private fun npgErrorsExpectedResponses(): Stream<Arguments> =
      Stream.of(
        Arguments.of(HttpStatus.BAD_REQUEST, RefundNotAllowedException::class.java),
        Arguments.of(HttpStatus.UNAUTHORIZED, RefundNotAllowedException::class.java),
        Arguments.of(HttpStatus.NOT_FOUND, RefundNotAllowedException::class.java),
        Arguments.of(HttpStatus.INTERNAL_SERVER_ERROR, BadGatewayException::class.java),
        Arguments.of(HttpStatus.GATEWAY_TIMEOUT, BadGatewayException::class.java),
      )
  }

  @Test
  fun requestRefund_200_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given(tracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.setParent(any())).willReturn(spanBuilder)
    given(spanBuilder.setAttribute(any<String>(), any<String>())).willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)
    mockWebServer.enqueue(
      MockResponse()
        .setBody(
          """
                    {
                        "operationId": "%s"
                    }
                """.format(
            operationId))
        .setHeader("Content-type", "application/json")
        .setResponseCode(200))
    // Test
    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .assertNext { assertEquals(operationId, it.operationId) }
      .verifyComplete()
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq("pspKey1"), any())
  }

  @ParameterizedTest
  @MethodSource("npgErrorsExpectedResponses")
  fun `should handle npg error response`(
    errorHttpStatusCode: HttpStatus,
    expectedException: Class<out Throwable>
  ) {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    mockWebServer.enqueue(
      MockResponse()
        .setBody(
          """
                    {
                        "errors": []
                    }
                """)
        .setResponseCode(errorHttpStatusCode.value()))
    given(tracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.setParent(any())).willReturn(spanBuilder)
    given(spanBuilder.setAttribute(any<String>(), any<String>())).willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)

    // Test
    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(expectedException)
      .verify()
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq("pspKey1"), any())
  }

  @ParameterizedTest
  @MethodSource("npgErrorsExpectedResponses")
  fun `should handle npg error response without error body`(
    errorHttpStatusCode: HttpStatus,
    expectedException: Class<out Throwable>
  ) {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    mockWebServer.enqueue(MockResponse().setResponseCode(errorHttpStatusCode.value()))
    given(tracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.setParent(any())).willReturn(spanBuilder)
    given(spanBuilder.setAttribute(any<String>(), any<String>())).willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)

    // Test
    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(expectedException)
      .verify()
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq("pspKey1"), any())
  }

  @Test
  fun `should handle npg error without http response code info`() {
    val npgClient: NpgClient = mock()
    val refundService = RefundService(paymentGatewayClient, npgClient, npgPspApiKeys)
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .willReturn(
        Mono.error(
          NpgResponseException(
            "NPG error", listOf(), Optional.empty(), RuntimeException("NPG error"))))

    // Test
    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(RefundNotAllowedException::class.java)
      .verify()
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq("pspKey1"), any())
  }

  @Test
  fun `should not call NPG and return error for not configured PSP key`() {
    val npgClient: NpgClient = mock()
    val refundService = RefundService(paymentGatewayClient, npgClient, npgPspApiKeys)
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "unknown"
    // Precondition

    // Test
    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(NpgApiKeyMissingPspRequestedException::class.java)
      .verify()
    verify(npgClient, times(0)).refundPayment(any(), any(), any(), any(), any(), any())
  }

  @Test
  fun requestRefund_200_vpos() {
    val testUUID: UUID = UUID.randomUUID()

    // Precondition
    Mockito.`when`(paymentGatewayClient.requestVPosRefund(testUUID))
      .thenReturn(Mono.just(getMockedVPosRefundRequest(testUUID.toString())))
    // Test
    val response = refundService.requestVposRefund(testUUID.toString()).block()
    // Assertions
    assertEquals("CANCELLED", response?.status?.value)
  }

  @Test
  fun requestRefund_200_xpay() {
    val testUUID: UUID = UUID.randomUUID()

    // Precondition
    Mockito.`when`(paymentGatewayClient.requestXPayRefund(testUUID))
      .thenReturn(Mono.just(getMockedXPayRefundRequest(testUUID.toString())))

    // Test
    val response = refundService.requestXpayRefund(testUUID.toString()).block()

    // Assertions
    assertEquals("CANCELLED", response?.status?.value)
  }
}
