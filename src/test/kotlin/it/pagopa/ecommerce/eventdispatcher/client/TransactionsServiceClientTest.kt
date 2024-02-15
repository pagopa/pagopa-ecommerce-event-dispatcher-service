package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.transactionauthrequests.v1.api.TransactionsApi
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.nio.charset.StandardCharsets
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
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
    val expectedResponse = TransactionInfoDto()
    given(transactionApi.updateTransactionAuthorization(transactionId.base64(), updateAuthRequest))
      .willReturn(mono { expectedResponse })
    // test
    StepVerifier.create(
        transactionsServiceClient.patchAuthRequest(transactionId, updateAuthRequest))
      .expectNext(expectedResponse)
      .verifyComplete()
  }

  companion object {
    private val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    @JvmStatic
    fun `Transactions service error codes method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(HttpStatus.BAD_REQUEST, TransactionNotFound(transactionId.uuid)),
        Arguments.of(HttpStatus.UNAUTHORIZED, TransactionNotFound(transactionId.uuid)),
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
    val expectedResponse = TransactionInfoDto()
    given(transactionApi.updateTransactionAuthorization(transactionId.base64(), updateAuthRequest))
      .willReturn(
        Mono.error {
          WebClientResponseException.create(
            httpErrorCode.value(),
            httpErrorCode.reasonPhrase,
            HttpHeaders.EMPTY,
            ByteArray(0),
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
  }
}
