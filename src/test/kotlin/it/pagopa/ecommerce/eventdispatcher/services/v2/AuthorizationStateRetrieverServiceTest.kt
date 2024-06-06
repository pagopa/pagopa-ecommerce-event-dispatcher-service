package it.pagopa.ecommerce.eventdispatcher.services.v2

import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.exceptions.NpgApiKeyMissingPspRequestedException
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.*
import it.pagopa.ecommerce.commons.utils.NpgApiKeyConfiguration
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgBadRequestException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgServerErrorException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.reduceEvents
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import org.springframework.http.HttpStatus
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

class AuthorizationStateRetrieverServiceTest {

  private val npgClient: NpgClient = mock()

  private val npgApiKeyConfiguration: NpgApiKeyConfiguration = mock()

  private val authorizationStateRetrieverService =
    AuthorizationStateRetrieverService(
      npgClient = npgClient, npgApiKeyConfiguration = npgApiKeyConfiguration)

  @Test
  fun `Should retrieve transaction status successfully`() {
    // pre-conditions
    val pspId = "pspId"
    val pspApiKey = "pspApiKey"
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)
    val sessionId = "sessionId"
    val correlationId = UUID.randomUUID()
    val stateResponse = StateResponseDto().state(WorkflowStateDto.CARD_DATA_COLLECTION)
    given(npgApiKeyConfiguration[NpgClient.PaymentMethod.CARDS, pspId])
      .willReturn(Either.right(pspApiKey))
    given(npgClient.getState(any(), any(), any())).willReturn(mono { stateResponse })
    // test
    StepVerifier.create(
        authorizationStateRetrieverService.getStateNpg(
          transactionId = transactionId,
          sessionId = sessionId,
          pspId = pspId,
          correlationId = correlationId.toString(),
          paymentMethod = NpgClient.PaymentMethod.CARDS))
      .expectNext(stateResponse)
      .verifyComplete()
    verify(npgClient, times(1)).getState(correlationId, sessionId, pspApiKey)
  }

  companion object {
    private val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)

    @JvmStatic
    fun `NPG get state error mapping method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          Optional.of(HttpStatus.BAD_REQUEST),
          NpgBadRequestException(
            transactionId, "Received HTTP error code from NPG: ${HttpStatus.BAD_REQUEST}")),
        Arguments.of(
          Optional.of(HttpStatus.NOT_FOUND),
          NpgBadRequestException(
            transactionId, "Received HTTP error code from NPG: ${HttpStatus.NOT_FOUND}")),
        Arguments.of(
          Optional.of(HttpStatus.INTERNAL_SERVER_ERROR),
          NpgServerErrorException(
            "Received HTTP error code from NPG: ${HttpStatus.INTERNAL_SERVER_ERROR}")),
        Arguments.of(
          Optional.empty<HttpStatus>(),
          NpgResponseException("Error communicating with NPG", Optional.empty(), Error())),
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
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)
    val sessionId = "sessionId"
    val correlationId = UUID.randomUUID()
    given(npgApiKeyConfiguration[NpgClient.PaymentMethod.CARDS, pspId])
      .willReturn(Either.right(pspApiKey))
    given(npgClient.getState(any(), any(), any()))
      .willReturn(
        Mono.error(
          NpgResponseException(
            "Error communicating with NPG", npgHttpErrorStatus, RuntimeException())))
    // test
    StepVerifier.create(
        authorizationStateRetrieverService.getStateNpg(
          transactionId = transactionId,
          sessionId = sessionId,
          pspId = pspId,
          correlationId = correlationId.toString(),
          paymentMethod = NpgClient.PaymentMethod.CARDS))
      .expectErrorMatches {
        assertEquals(expectedExceptionToBeThrown::class.java, it::class.java)
        assertEquals(expectedExceptionToBeThrown.message, it.message)
        true
      }
      .verify()
    verify(npgClient, times(1)).getState(correlationId, sessionId, pspApiKey)
  }

  @Test
  fun `Should throw error for psp api key not configured`() {
    // pre-conditions
    val pspId = "pspId"
    val pspApiKey = "pspApiKey"
    val transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID)
    val sessionId = "sessionId"
    val correlationId = UUID.randomUUID()
    val stateResponse = StateResponseDto().state(WorkflowStateDto.CARD_DATA_COLLECTION)
    given(npgApiKeyConfiguration[NpgClient.PaymentMethod.CARDS, pspId])
      .willReturn(Either.left(NpgApiKeyMissingPspRequestedException(pspId, setOf())))
    given(npgClient.getState(any(), any(), any())).willReturn(mono { stateResponse })
    // test
    StepVerifier.create(
        authorizationStateRetrieverService.getStateNpg(
          transactionId = transactionId,
          sessionId = sessionId,
          pspId = pspId,
          correlationId = correlationId.toString(),
          paymentMethod = NpgClient.PaymentMethod.CARDS))
      .expectError(NpgApiKeyMissingPspRequestedException::class.java)
      .verify()
    verify(npgClient, times(0)).getState(correlationId, sessionId, pspApiKey)
  }

  @Nested
  inner class NpgOrders {
    @Test
    fun `Should retrieve npg order successfully`() {
      // pre-conditions
      val correlationId = UUID.randomUUID()
      val stateResponse =
        OrderResponseDto()
          .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))
          .addOperationsItem(
            OperationDto()
              .operationId(UUID.randomUUID().toString())
              .orderId(UUID.randomUUID().toString())
              .operationType(OperationTypeDto.AUTHORIZATION)
              .operationResult(OperationResultDto.EXECUTED)
              .paymentEndToEndId(UUID.randomUUID().toString())
              .operationTime(ZonedDateTime.now().toString()))
      given(npgApiKeyConfiguration[NpgClient.PaymentMethod.CARDS, "pspId"])
        .willReturn(Either.right("pspApiKey"))
      given(npgClient.getOrder(any(), any(), any())).willReturn(mono { stateResponse })

      val transaction = transactionWithAuthRequested(correlationId)

      // test
      StepVerifier.create(
          authorizationStateRetrieverService.performGetOrder(
            transaction as BaseTransactionWithRequestedAuthorization))
        .expectNext(stateResponse)
        .verifyComplete()
      verify(npgClient, times(1))
        .getOrder(
          correlationId,
          "pspApiKey",
          transaction.transactionAuthorizationRequestData.authorizationRequestId)
    }

    @ParameterizedTest
    @MethodSource(
      "it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverServiceTest#NPG get state error mapping method source")
    fun `Should handle error retrieving order from NPG`(
      npgHttpErrorStatus: Optional<HttpStatus>,
      expectedExceptionToBeThrown: Exception
    ) {
      // pre-conditions
      val pspId = "pspId"
      val pspApiKey = "pspApiKey"
      val correlationId = UUID.randomUUID()
      given(npgApiKeyConfiguration[NpgClient.PaymentMethod.CARDS, pspId])
        .willReturn(Either.right(pspApiKey))
      given(npgClient.getOrder(any(), any(), any()))
        .willReturn(
          Mono.error(
            NpgResponseException(
              "Error communicating with NPG", npgHttpErrorStatus, RuntimeException())))
      // test
      val transaction = transactionWithAuthRequested(correlationId)

      StepVerifier.create(authorizationStateRetrieverService.performGetOrder(transaction))
        .expectErrorMatches {
          assertEquals(expectedExceptionToBeThrown::class.java, it::class.java)
          assertEquals(expectedExceptionToBeThrown.message, it.message)
          true
        }
        .verify()
      verify(npgClient, times(1))
        .getOrder(
          correlationId,
          pspApiKey,
          transaction.transactionAuthorizationRequestData.authorizationRequestId)
    }
  }

  private fun transactionWithAuthRequested(
    correlationId: UUID
  ): BaseTransactionWithRequestedAuthorization {
    return reduceEvents(
        listOf(
            TransactionTestUtils.transactionActivateEvent(
              NpgTransactionGatewayActivationData("orderId", correlationId.toString())),
            TransactionTestUtils.transactionAuthorizationRequestedEvent(
              TransactionAuthorizationRequestData.PaymentGateway.NPG,
              TransactionTestUtils.npgTransactionGatewayAuthorizationRequestedData()))
          .map { it as TransactionEvent<Any> }
          .toFlux())
      .block() as BaseTransactionWithRequestedAuthorization
  }
}
