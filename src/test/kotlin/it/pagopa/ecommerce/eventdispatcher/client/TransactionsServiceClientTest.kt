package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.generated.transactionauthrequests.v2.api.TransactionsApi
import it.pagopa.generated.transactionauthrequests.v2.dto.UpdateAuthorizationRequestDto
import it.pagopa.generated.transactionauthrequests.v2.dto.UpdateAuthorizationResponseDto
import java.nio.charset.StandardCharsets
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TransactionsServiceClientTest {

  private val transactionApi: TransactionsApi = mock()

  private val transactionsServiceClient = TransactionsServiceClient(transactionApi)

  @Test
  fun `Should update transaction successfully`() {
    // pre-requisites
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)
    val updateAuthRequest = UpdateAuthorizationRequestDto()
    val expectedResponse = UpdateAuthorizationResponseDto()
    given(transactionApi.updateTransactionAuthorization(any(), any()))
      .willReturn(mono { expectedResponse })
    // test
    StepVerifier.create(
        transactionsServiceClient.patchAuthRequest(transactionId, updateAuthRequest))
      .expectNext(expectedResponse)
      .verifyComplete()
    verify(transactionApi, times(1))
      .updateTransactionAuthorization(transactionId.value(), updateAuthRequest)
  }

  companion object {
    private val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    @JvmStatic
    fun `Transactions service error codes method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          HttpStatus.BAD_REQUEST,
          PatchAuthRequestErrorResponseException(
            transactionId, HttpStatus.BAD_REQUEST, "ErrorMessage")),
        Arguments.of(
          HttpStatus.UNAUTHORIZED,
          UnauthorizedPatchAuthorizationRequestException(transactionId, HttpStatus.UNAUTHORIZED)),
        Arguments.of(HttpStatus.NOT_FOUND, TransactionNotFound(transactionId.uuid)),
        Arguments.of(HttpStatus.GATEWAY_TIMEOUT, GatewayTimeoutException()),
        Arguments.of(HttpStatus.INTERNAL_SERVER_ERROR, BadGatewayException("")),
        Arguments.of(HttpStatus.BAD_GATEWAY, BadGatewayException("")),
        Arguments.of(
          HttpStatus.REQUEST_TIMEOUT,
          WebClientResponseException.create(
            HttpStatus.REQUEST_TIMEOUT.value(),
            HttpStatus.REQUEST_TIMEOUT.reasonPhrase,
            HttpHeaders.EMPTY,
            ByteArray(0),
            StandardCharsets.UTF_8)),
      )
  }

  @ParameterizedTest
  @MethodSource("Transactions service error codes method source")
  fun `Should handle transactions service error codes successfully`(
    httpErrorCode: HttpStatus,
    expectedException: Exception
  ) {
    // pre-requisites
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)
    val updateAuthRequest = UpdateAuthorizationRequestDto()
    given(transactionApi.updateTransactionAuthorization(any(), any()))
      .willReturn(
        Mono.error {
          WebClientResponseException.create(
            httpErrorCode.value(),
            httpErrorCode.reasonPhrase,
            HttpHeaders.EMPTY,
            "ErrorMessage".encodeToByteArray(),
            StandardCharsets.UTF_8)
        })
    // test
    StepVerifier.create(
        transactionsServiceClient.patchAuthRequest(transactionId, updateAuthRequest))
      .expectErrorMatches {
        assertEquals(expectedException::class.java, it::class.java)
        assertEquals(expectedException.message, it.message)
        true
      }
      .verify()
    verify(transactionApi, times(1))
      .updateTransactionAuthorization(transactionId.value(), updateAuthRequest)
  }
}
