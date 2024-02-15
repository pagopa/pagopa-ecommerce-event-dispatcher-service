package it.pagopa.ecommerce.eventdispatcher.services.v2

import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.WorkflowStateDto
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GetStateException
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import org.springframework.http.HttpStatus
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class NpgStateServiceTest {

  private val npgClient: NpgClient = mock()

  private val npgPspApiKeysConfig: NpgPspApiKeysConfig = mock()

  private val npgStateService =
    NpgStateService(npgClient = npgClient, npgCardsPspApiKey = npgPspApiKeysConfig)

  @Test
  fun `Should retrieve transaction status successfully`() {
    // pre-conditions
    val pspId = "pspId"
    val pspApiKey = "pspApiKey"
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID).uuid
    val sessionId = "sessionId"
    val correlationId = UUID.randomUUID()
    val stateResponse = StateResponseDto().state(WorkflowStateDto.CARD_DATA_COLLECTION)
    given(npgPspApiKeysConfig[pspId]).willReturn(Either.right(pspApiKey))
    given(npgClient.getState(any(), any(), any())).willReturn(mono { stateResponse })
    // test
    StepVerifier.create(
        npgStateService.getStateNpg(
          transactionId = transactionId,
          sessionId = sessionId,
          pspId = pspId,
          correlationId = correlationId.toString()))
      .expectNext(stateResponse)
      .verifyComplete()
    verify(npgClient, times(1)).getState(correlationId, sessionId, pspApiKey)
  }

  companion object {
    private val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID).uuid

    @JvmStatic
    fun `NPG get state error mapping method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          Optional.of(HttpStatus.BAD_REQUEST),
          GetStateException(
            transactionId, "Received HTTP error code from NPG: ${HttpStatus.BAD_REQUEST}")),
        Arguments.of(
          Optional.of(HttpStatus.NOT_FOUND),
          GetStateException(
            transactionId, "Received HTTP error code from NPG: ${HttpStatus.NOT_FOUND}")),
        Arguments.of(
          Optional.of(HttpStatus.INTERNAL_SERVER_ERROR),
          BadGatewayException(
            "Received HTTP error code from NPG: ${HttpStatus.INTERNAL_SERVER_ERROR}")),
        Arguments.of(
          Optional.empty<HttpStatus>(),
          GetStateException(transactionId, "Unknown NPG HTTP response code")),
      )
  }

  @ParameterizedTest
  @MethodSource("NPG get state error mapping method source")
  fun `Should handle error retrieving transaction status from NPG`(
    npgHttpErrorStatus: Optional<HttpStatus>,
    expectedExceptionToBeThrown: Exception
  ) {
    // pre-conditions
    val pspId = "pspId"
    val pspApiKey = "pspApiKey"
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID).uuid
    val sessionId = "sessionId"
    val correlationId = UUID.randomUUID()
    given(npgPspApiKeysConfig[pspId]).willReturn(Either.right(pspApiKey))
    given(npgClient.getState(any(), any(), any()))
      .willReturn(
        Mono.error(
          NpgResponseException(
            "Error communicating with NPG", npgHttpErrorStatus, RuntimeException())))
    // test
    StepVerifier.create(
        npgStateService.getStateNpg(
          transactionId = transactionId,
          sessionId = sessionId,
          pspId = pspId,
          correlationId = correlationId.toString()))
      .expectErrorMatches {
        assertEquals(expectedExceptionToBeThrown::class.java, it::class.java)
        assertEquals(expectedExceptionToBeThrown.message, it.message)
        true
      }
      .verify()
    verify(npgClient, times(1)).getState(correlationId, sessionId, pspApiKey)
  }
}
