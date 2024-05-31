package it.pagopa.ecommerce.eventdispatcher.services

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.client.NodeForwarderClient
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.exceptions.NodeForwarderClientException
import it.pagopa.ecommerce.commons.exceptions.NpgApiKeyConfigurationException
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.exceptions.RedirectConfigurationException
import it.pagopa.ecommerce.commons.utils.NpgApiKeyConfiguration
import it.pagopa.ecommerce.commons.utils.RedirectKeysConfiguration
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.config.RedirectConfigurationBuilder
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedVPosRefundRequest
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedXPayRefundRequest
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundOutcomeDto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundRequestDto as RedirectRefundRequestDto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto as RedirectRefundResponseDto
import java.math.BigDecimal
import java.net.URI
import java.util.*
import java.util.stream.Stream
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.TestPropertySource
import org.springframework.web.reactive.function.client.WebClientResponseException
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

  private val npgApiKeyConfiguration =
    NpgApiKeyConfiguration.Builder()
      .setDefaultApiKey("defaultApiKey")
      .withMethodPspMapping(
        NpgClient.PaymentMethod.CARDS,
        """
      {
        "pspId1": "pspKey1"
      }
    """,
        setOf("pspId1"),
        jacksonObjectMapper())
      .build()

  private val nodeForwarderRedirectApiClient:
    NodeForwarderClient<RedirectRefundRequestDto, RedirectRefundResponseDto> =
    mock()

  private val redirectBeApiCallUriMap: Map<String, String> =
    mapOf("pspId-RPIC" to "http://redirect/RPIC")
  private val redirectBeAoiCallUriSet: Set<String> = setOf("pspId-RPIC")
  private val redirectKeysConfiguration: RedirectKeysConfiguration =
    RedirectKeysConfiguration(redirectBeApiCallUriMap, redirectBeAoiCallUriSet)
  private val refundService: RefundService =
    RefundService(
      paymentGatewayClient = paymentGatewayClient,
      npgClient = npgClient,
      npgApiKeyConfiguration = npgApiKeyConfiguration,
      nodeForwarderRedirectApiClient = nodeForwarderRedirectApiClient,
      redirectBeApiCallUriConf = redirectKeysConfiguration)

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

    @JvmStatic
    private fun `Redirect refund errors method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(HttpStatus.BAD_REQUEST, RefundNotAllowedException::class.java),
        Arguments.of(HttpStatus.UNAUTHORIZED, RefundNotAllowedException::class.java),
        Arguments.of(HttpStatus.NOT_FOUND, RefundNotAllowedException::class.java),
        Arguments.of(HttpStatus.INTERNAL_SERVER_ERROR, BadGatewayException::class.java),
        Arguments.of(HttpStatus.GATEWAY_TIMEOUT, BadGatewayException::class.java),
        Arguments.of(null, BadGatewayException::class.java),
      )

    @JvmStatic
    private fun redirectRetrieveUrlPaymentMethodsTestSearch(): Stream<Arguments> {
      return Stream.of<Arguments>(
        Arguments.of(
          "CHECKOUT", "psp1", "RBPR", URI("http://localhost:8096/redirections1/CHECKOUT")),
        Arguments.of("IO", "psp1", "RBPR", URI("http://localhost:8096/redirections1/IO")),
        Arguments.of("CHECKOUT", "psp2", "RBPB", URI("http://localhost:8096/redirections2")),
        Arguments.of("IO", "psp2", "RBPB", URI("http://localhost:8096/redirections2")),
        Arguments.of("CHECKOUT", "psp3", "RBPS", URI("http://localhost:8096/redirections3")),
        Arguments.of("IO", "psp3", "RBPS", URI("http://localhost:8096/redirections3")))
    }
  }

  @Test
  fun requestRefund_200_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val correlationId = UUID.randomUUID().toString()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given(tracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.setParent(any())).willReturn(spanBuilder)
    given(spanBuilder.setAttribute(eq(AttributeKey.stringKey("npg.correlation_id")), any()))
      .willReturn(spanBuilder)
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
    StepVerifier.create(
        refundService.requestNpgRefund(
          operationId = operationId,
          idempotenceKey = idempotenceKey,
          amount = amount,
          pspId = pspId,
          correlationId = correlationId,
          paymentMethod = NpgClient.PaymentMethod.CARDS))
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
    val correlationId = UUID.randomUUID().toString()
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
    given(spanBuilder.setAttribute(eq(AttributeKey.stringKey("npg.correlation_id")), any()))
      .willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)

    // Test
    StepVerifier.create(
        refundService.requestNpgRefund(
          operationId = operationId,
          idempotenceKey = idempotenceKey,
          amount = amount,
          pspId = pspId,
          correlationId = correlationId,
          paymentMethod = NpgClient.PaymentMethod.CARDS))
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
    val correlationId = UUID.randomUUID().toString()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    mockWebServer.enqueue(MockResponse().setResponseCode(errorHttpStatusCode.value()))
    given(tracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.setParent(any())).willReturn(spanBuilder)
    given(spanBuilder.setAttribute(eq(AttributeKey.stringKey("npg.correlation_id")), any()))
      .willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)

    // Test
    StepVerifier.create(
        refundService.requestNpgRefund(
          operationId = operationId,
          idempotenceKey = idempotenceKey,
          amount = amount,
          pspId = pspId,
          correlationId = correlationId,
          paymentMethod = NpgClient.PaymentMethod.CARDS))
      .expectError(expectedException)
      .verify()
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq("pspKey1"), any())
  }

  @Test
  fun `should handle npg error without http response code info`() {
    val npgClient: NpgClient = mock()
    val refundService =
      RefundService(
        paymentGatewayClient = paymentGatewayClient,
        npgClient = npgClient,
        npgApiKeyConfiguration = npgApiKeyConfiguration,
        nodeForwarderRedirectApiClient = nodeForwarderRedirectApiClient,
        redirectBeApiCallUriConf = redirectKeysConfiguration)
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val correlationId = UUID.randomUUID().toString()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .willReturn(
        Mono.error(
          NpgResponseException(
            "NPG error", listOf(), Optional.empty(), RuntimeException("NPG error"))))

    // Test
    StepVerifier.create(
        refundService.requestNpgRefund(
          operationId = operationId,
          idempotenceKey = idempotenceKey,
          amount = amount,
          pspId = pspId,
          correlationId = correlationId,
          paymentMethod = NpgClient.PaymentMethod.CARDS))
      .expectError(NpgResponseException::class.java)
      .verify()
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq("pspKey1"), any())
  }

  @Test
  fun `should not call NPG and return error for not configured PSP key`() {
    val npgClient: NpgClient = mock()
    val refundService =
      RefundService(
        paymentGatewayClient = paymentGatewayClient,
        npgClient = npgClient,
        npgApiKeyConfiguration = npgApiKeyConfiguration,
        nodeForwarderRedirectApiClient = nodeForwarderRedirectApiClient,
        redirectBeApiCallUriConf = redirectKeysConfiguration)
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val correlationId = UUID.randomUUID().toString()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "unknown"
    // Precondition

    // Test
    StepVerifier.create(
        refundService.requestNpgRefund(
          operationId = operationId,
          idempotenceKey = idempotenceKey,
          amount = amount,
          pspId = pspId,
          correlationId = correlationId,
          paymentMethod = NpgClient.PaymentMethod.CARDS))
      .expectError(NpgApiKeyConfigurationException::class.java)
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

  @Test
  fun `Should perform redirect refund successfully`() {
    // pre-requisites
    val transactionId = TransactionTestUtils.TRANSACTION_ID
    val pspTransactionId = "pspTransactionId"
    val paymentTypeCode = "RPIC"
    val pspId = "pspId"
    val touchpoint = "CHECKOUT"
    val redirectRefundResponse =
      RedirectRefundResponseDto().idTransaction(transactionId).outcome(RefundOutcomeDto.OK)
    val expectedRequest =
      RedirectRefundRequestDto()
        .action("refund")
        .idPSPTransaction(pspTransactionId)
        .idTransaction(transactionId)
    given(nodeForwarderRedirectApiClient.proxyRequest(any(), any(), any(), any()))
      .willReturn(
        Mono.just(
          NodeForwarderClient.NodeForwarderResponse(redirectRefundResponse, Optional.empty())))
    // test
    StepVerifier.create(
        refundService.requestRedirectRefund(
          transactionId = TransactionId(transactionId),
          touchpoint = touchpoint,
          pspTransactionId = pspTransactionId,
          paymentTypeCode = paymentTypeCode,
          pspId = pspId))
      .expectNext(redirectRefundResponse)
      .verifyComplete()
    verify(nodeForwarderRedirectApiClient, times(1))
      .proxyRequest(
        expectedRequest,
        redirectBeApiCallUriMap["pspId-$paymentTypeCode"]?.let { URI(it) },
        transactionId,
        RedirectRefundResponseDto::class.java)
  }

  @Test
  fun `Should return error performing redirect refund for missing backend URL`() {
    // pre-requisites
    val transactionId = TransactionTestUtils.TRANSACTION_ID
    val pspTransactionId = "pspTransactionId"
    val paymentTypeCode = "MISSING"
    val touchpoint = "CHECKOUT"
    // test
    StepVerifier.create(
        refundService.requestRedirectRefund(
          transactionId = TransactionId(transactionId),
          touchpoint = touchpoint,
          pspTransactionId = pspTransactionId,
          paymentTypeCode = paymentTypeCode,
          pspId = "pspId"))
      .expectErrorMatches {
        assertTrue(it is RedirectConfigurationException)
        assertEquals(
          "Error parsing Redirect PSP BACKEND_URLS configuration, cause: Missing key for redirect return url with following search parameters: touchpoint: [CHECKOUT] pspId: [pspId] paymentTypeCode: [MISSING]",
          it.message)
        true
      }
      .verify()
    verify(nodeForwarderRedirectApiClient, times(0)).proxyRequest(any(), any(), any(), any())
  }

  @ParameterizedTest
  @MethodSource("Redirect refund errors method source")
  fun `Should handle returned error performing redirect refund call`(
    httpErrorCode: HttpStatus?,
    expectedErrorClass: Class<Exception>
  ) {
    // pre-requisites
    val transactionId = TransactionTestUtils.TRANSACTION_ID
    val pspTransactionId = "pspTransactionId"
    val touchpoint = "CHECKOUT"
    val paymentTypeCode = "RPIC"
    val pspId = "pspId"
    val expectedRequest =
      RedirectRefundRequestDto()
        .action("refund")
        .idPSPTransaction(pspTransactionId)
        .idTransaction(transactionId)
    given(nodeForwarderRedirectApiClient.proxyRequest(any(), any(), any(), any()))
      .willReturn(
        Mono.error(
          NodeForwarderClientException(
            "Error performing refund",
            if (httpErrorCode != null) {
              WebClientResponseException(
                "Error performing request",
                httpErrorCode.value(),
                "",
                HttpHeaders.EMPTY,
                null,
                null)
            } else {
              RuntimeException("Error performing request")
            })))
    // test
    StepVerifier.create(
        refundService.requestRedirectRefund(
          transactionId = TransactionId(transactionId),
          touchpoint = touchpoint,
          pspTransactionId = pspTransactionId,
          paymentTypeCode = paymentTypeCode,
          pspId = pspId))
      .expectError(expectedErrorClass)
      .verify()
    verify(nodeForwarderRedirectApiClient, times(1))
      .proxyRequest(
        expectedRequest,
        redirectBeApiCallUriMap["pspId-$paymentTypeCode"]?.let { URI(it) },
        transactionId,
        RedirectRefundResponseDto::class.java)
  }

  @ParameterizedTest
  @MethodSource("redirectRetrieveUrlPaymentMethodsTestSearch")
  fun `Should return URI during search redirectURL searching iteratively`(
    touchpoint: String,
    pspId: String,
    paymentMethodId: String,
    expectedUri: URI
  ) {
    val redirectUrlMapping =
      java.util.Map.of(
        "CHECKOUT-psp1-RBPR",
        "http://localhost:8096/redirections1/CHECKOUT",
        "IO-psp1-RBPR",
        "http://localhost:8096/redirections1/IO",
        "psp2-RBPB",
        "http://localhost:8096/redirections2",
        "RBPS",
        "http://localhost:8096/redirections3")
    val codeTypeList = setOf("CHECKOUT-psp1-RBPR", "IO-psp1-RBPR", "psp2-RBPB", "RBPS")
    val transactionId = TransactionTestUtils.TRANSACTION_ID
    val pspTransactionId = "pspTransactionId"
    val redirectRefundResponse =
      RedirectRefundResponseDto().idTransaction(transactionId).outcome(RefundOutcomeDto.OK)

    val refundServiceTest =
      RefundService(
        paymentGatewayClient = paymentGatewayClient,
        npgClient = npgClient,
        npgApiKeyConfiguration = npgApiKeyConfiguration,
        nodeForwarderRedirectApiClient = nodeForwarderRedirectApiClient,
        redirectBeApiCallUriConf =
          RedirectConfigurationBuilder().redirectBeApiCallUriConf(redirectUrlMapping, codeTypeList))

    given(nodeForwarderRedirectApiClient.proxyRequest(any(), any(), any(), any()))
      .willReturn(
        Mono.just(
          NodeForwarderClient.NodeForwarderResponse(redirectRefundResponse, Optional.empty())))

    StepVerifier.create(
        refundServiceTest.requestRedirectRefund(
          transactionId = TransactionId(transactionId),
          touchpoint = touchpoint,
          pspTransactionId = pspTransactionId,
          paymentTypeCode = paymentMethodId,
          pspId = pspId))
      .expectNext(redirectRefundResponse)
      .verifyComplete()

    Mockito.verify(nodeForwarderRedirectApiClient, Mockito.times(1))
      .proxyRequest(any(), eq(URI.create("$expectedUri/refunds")), any(), any())
  }

  @Test
  fun `Should return error during search redirect URL for invalid search key`() {
    val redirectUrlMapping =
      java.util.Map.of(
        "CHECKOUT-psp1-RBPR",
        "http://localhost:8096/redirections1/CHECKOUT",
        "IO-psp1-RBPR",
        "http://localhost:8096/redirections1/IO",
        "psp2-RBPB",
        "http://localhost:8096/redirections2",
        "RBPS",
        "http://localhost:8096/redirections3")
    val codeTypeList = setOf("CHECKOUT-psp1-RBPR", "IO-psp1-RBPR", "psp2-RBPB", "RBPS")
    val transactionId = TransactionTestUtils.TRANSACTION_ID
    val pspTransactionId = "pspTransactionId"
    val redirectRefundResponse =
      RedirectRefundResponseDto().idTransaction(transactionId).outcome(RefundOutcomeDto.OK)
    val touchpoint = "CHECKOUT"
    val pspId = "pspId"
    val paymentTypeCode = "RBPP"

    val refundServiceTest =
      RefundService(
        paymentGatewayClient = paymentGatewayClient,
        npgClient = npgClient,
        npgApiKeyConfiguration = npgApiKeyConfiguration,
        nodeForwarderRedirectApiClient = nodeForwarderRedirectApiClient,
        redirectBeApiCallUriConf =
          RedirectConfigurationBuilder().redirectBeApiCallUriConf(redirectUrlMapping, codeTypeList))

    given(nodeForwarderRedirectApiClient.proxyRequest(any(), any(), any(), any()))
      .willReturn(
        Mono.just(
          NodeForwarderClient.NodeForwarderResponse(redirectRefundResponse, Optional.empty())))

    StepVerifier.create(
        refundServiceTest.requestRedirectRefund(
          transactionId = TransactionId(transactionId),
          touchpoint = touchpoint,
          pspTransactionId = pspTransactionId,
          paymentTypeCode = paymentTypeCode,
          pspId = pspId))
      .consumeErrorWith {
        assertEquals(
          "Error parsing Redirect PSP BACKEND_URLS configuration, cause: Missing key for redirect return url with following search parameters: touchpoint: [${touchpoint}] pspId: [${pspId}] paymentTypeCode: [${paymentTypeCode}]",
          it.message)
      }
      .verify()
  }
}
