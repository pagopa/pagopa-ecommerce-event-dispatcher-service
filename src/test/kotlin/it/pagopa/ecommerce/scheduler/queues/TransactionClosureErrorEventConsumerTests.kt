package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.scheduler.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.ZonedDateTime
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionClosureErrorEventConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val nodeService: NodeService = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val transactionClosedEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData> =
    mock()

  private val transactionClosureErrorEventsConsumer =
    TransactionClosureErrorEventConsumer(
      transactionsEventStoreRepository,
      transactionClosedEventRepository,
      transactionsViewRepository,
      nodeService)

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      val uuidFromStringWorkaround =
        "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently
      // not working
      val expectedClosureEvent =
        TransactionClosedEvent(
          uuidFromStringWorkaround, TransactionClosureData(TransactionClosureData.Outcome.OK))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(any()))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(
          nodeService.closePayment(
            UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      /* test */

      val closureEventId = UUID.fromString(expectedClosureEvent.id)

      Mockito.mockStatic(UUID::class.java).use { uuid ->
        uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
        uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

        transactionClosureErrorEventsConsumer
          .messageReceiver(BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer)
          .block()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
        verify(nodeService, Mockito.times(1))
          .closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
        verify(transactionClosedEventRepository, Mockito.times(1))
          .save(
            any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
        // mocking
        verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      }
    }

  @Test
  fun `consumer processes bare closure error message correctly with KO closure outcome for authorization completed transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val uuidFromStringWorkaround =
        "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently
      // not working

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      val expectedClosureEvent =
        TransactionClosureFailedEvent(
          activationEvent.transactionId, TransactionClosureData(TransactionClosureData.Outcome.OK))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(any()))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(
          nodeService.closePayment(
            UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      val closureEventId = UUID.fromString(expectedClosureEvent.id)

      Mockito.mockStatic(UUID::class.java).use { uuid ->
        uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
        uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

        transactionClosureErrorEventsConsumer
          .messageReceiver(BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer)
          .block()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
        verify(nodeService, Mockito.times(1))
          .closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
        verify(transactionClosedEventRepository, Mockito.times(1))
          .save(
            any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
        // mocking
        verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      }
    }

  @Test
  fun `consumer processes bare closure error message correctly with KO closure outcome for user canceled transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val userCanceledEvent = transactionUserCanceledEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val uuidFromStringWorkaround =
        "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently
      // not working

      val events = listOf(activationEvent, userCanceledEvent, closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      val expectedClosureEvent =
        TransactionClosureFailedEvent(
          activationEvent.transactionId, TransactionClosureData(TransactionClosureData.Outcome.OK))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(any()))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(
          nodeService.closePayment(
            UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      val closureEventId = UUID.fromString(expectedClosureEvent.id)

      Mockito.mockStatic(UUID::class.java).use { uuid ->
        uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
        uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

        transactionClosureErrorEventsConsumer
          .messageReceiver(BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer)
          .block()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
        verify(nodeService, Mockito.times(1))
          .closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
        verify(transactionClosedEventRepository, Mockito.times(1))
          .save(
            any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
        // mocking
        verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      }
    }

  @Test
  fun `consumer error processing bare closure error message for authorized transaction with missing authorizationResultDto value`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(null) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val uuidFromStringWorkaround =
        "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently
      // not working

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      val expectedClosureEvent =
        TransactionClosureFailedEvent(
          activationEvent.transactionId, TransactionClosureData(TransactionClosureData.Outcome.OK))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(any()))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(
          nodeService.closePayment(
            UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      val closureEventId = UUID.fromString(expectedClosureEvent.id)

      Mockito.mockStatic(UUID::class.java).use { uuid ->
        uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
        uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()
        assertThrows<RuntimeException> {
          transactionClosureErrorEventsConsumer
            .messageReceiver(BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer)
            .block()
        }

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
        verify(nodeService, Mockito.times(0))
          .closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
        verify(transactionClosedEventRepository, Mockito.times(0))
          .save(
            any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
        // mocking
        verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      }
    }

  @Test
  fun `consumer error processing bare closure error message for closure error aggregate with unexpected transactionAtPreviousStep`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val emptyTransactionMock: EmptyTransaction = mock()
      val transactionWithClosureError: TransactionWithClosureError = mock()
      val fakeTransactionAtPreviousState = transactionActivated(ZonedDateTime.now().toString())

      val uuidFromStringWorkaround =
        "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently
      // not working

      val events = listOf(activatedEvent as TransactionEvent<Any>)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
      given(emptyTransactionMock.applyEvent(any())).willReturn(transactionWithClosureError)
      given(transactionWithClosureError.transactionId)
        .willReturn(TransactionId(UUID.fromString(TRANSACTION_ID)))
      given(transactionWithClosureError.status).willReturn(TransactionStatusDto.CLOSURE_ERROR)
      given(transactionWithClosureError.transactionAtPreviousState)
        .willReturn(fakeTransactionAtPreviousState)

      /* test */

      assertThrows<RuntimeException> {
        transactionClosureErrorEventsConsumer
          .processMessage(
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer, emptyTransactionMock)
          .block()
      }

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0))
        .closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    }

  @Test
  fun `consumer processes closure retry message correctly`() = runTest {
    val closureRetriedEvent = transactionClosureRetriedEvent(0)

    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK) as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent, authorizationRequestEvent, authorizationUpdateEvent, closureErrorEvent)

    val expectedUpdatedTransaction =
      transactionDocument(
        TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    val uuidFromStringWorkaround =
      "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently not
    // working
    val expectedClosureEvent =
      TransactionClosedEvent(
        uuidFromStringWorkaround, TransactionClosureData(TransactionClosureData.Outcome.OK))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(any()))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionClosedEventRepository.save(any())).willReturn(Mono.just(expectedClosureEvent))
    given(
        nodeService.closePayment(
          UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    /* test */

    val closureEventId = UUID.fromString(expectedClosureEvent.id)

    Mockito.mockStatic(UUID::class.java).use { uuid ->
      uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
      uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

      transactionClosureErrorEventsConsumer
        .messageReceiver(BinaryData.fromObject(closureRetriedEvent).toBytes(), checkpointer)
        .block()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
    }
  }

  @Test
  fun `consumer process doesn't modify db on invalid transaction status`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK) as TransactionEvent<Any>

    val events = listOf(activationEvent, authorizationRequestEvent, authorizationUpdateEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(any()))
      .willReturn(Mono.just(transactionDocument))

    /* test */
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    assertThrows<BadTransactionStatusException> {
      transactionClosureErrorEventsConsumer
        .messageReceiver(BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer)
        .block()
    }

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(0)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
  }
}
