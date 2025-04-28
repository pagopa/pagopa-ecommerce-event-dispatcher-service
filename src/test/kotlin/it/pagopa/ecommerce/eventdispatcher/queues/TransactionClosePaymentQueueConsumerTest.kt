package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.nio.charset.StandardCharsets
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TransactionClosePaymentQueueConsumerTest {

  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionClosePaymentQueueConsumer =
    mock()
  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val queueConsumerV2Captor:
    KArgumentCaptor<
      Either<
        QueueEvent<it.pagopa.ecommerce.commons.documents.v2.TransactionUserCanceledEvent>,
        QueueEvent<it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRequestedEvent>>> =
    argumentCaptor<
      Either<
        QueueEvent<it.pagopa.ecommerce.commons.documents.v2.TransactionUserCanceledEvent>,
        QueueEvent<it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRequestedEvent>>>()

  private val checkpointer: Checkpointer = mock()

  private val transactionClosePaymentQueueConsumer =
    spy(
      TransactionClosePaymentQueueConsumer(
        queueConsumerV2 = queueConsumerV2,
        deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
        strictSerializerProviderV1 = strictSerializerProviderV1,
        strictSerializerProviderV2 = strictSerializerProviderV2))

  companion object {
    private val queuesConsumerConfig = QueuesConsumerConfig()

    val strictSerializerProviderV1 = queuesConsumerConfig.strictSerializerProviderV1()

    val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()
    private val transactionUserCanceledEventV2 =
      it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionUserCanceledEvent()
    private val transactionClosureRequestedEvent =
      it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionClosureRequestedEvent()
    private val tracingInfo = TracingInfoTest.MOCK_TRACING_INFO

    @JvmStatic
    fun eventToHandleTestV2(): Stream<Arguments> {

      return Stream.of(
        Arguments.of(
          String(
            BinaryData.fromObject(
                QueueEvent(transactionUserCanceledEventV2, tracingInfo),
                strictSerializerProviderV2.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          transactionUserCanceledEventV2),
        Arguments.of(
          String(
            BinaryData.fromObject(
                QueueEvent(transactionClosureRequestedEvent, tracingInfo),
                strictSerializerProviderV2.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          transactionClosureRequestedEvent),
      )
    }
  }

  @ParameterizedTest
  @MethodSource("eventToHandleTestV2")
  fun `Should dispatch TransactionV2 events`(
    serializedEvent: String,
    originalEvent: BaseTransactionEvent<*>
  ) = runTest {
    // pre-condition
    println("Serialized event: $serializedEvent")
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosePaymentQueueConsumer.messageReceiver(
          serializedEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .verifyComplete()
    // assertions
    verify(queueConsumerV2, times(1)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any())
    val queueEvent = queueConsumerV2Captor.firstValue
    assertEquals(originalEvent, queueEvent.fold({ it }, { it }).event)
    assertNotNull(queueEvent.fold({ it }, { it }).tracingInfo)
  }

  @Test
  fun `Should write event to dead letter queue for invalid event received`() = runTest {
    // pre-condition
    val invalidEvent = "test"
    val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosePaymentQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
  }

  @Test
  fun `Should handle error writing event to dead letter queue for invalid event received`() =
    runTest {
      // pre-condition
      val invalidEvent = "test"
      val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
      given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
        .willReturn(Mono.empty())
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(Mono.error(RuntimeException("Error writing event to queue")))
      // test
      Hooks.onOperatorDebug()
      StepVerifier.create(
          transactionClosePaymentQueueConsumer.messageReceiver(
            invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
        .expectError(java.lang.RuntimeException::class.java)
        .verify()
      // assertions
      verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> { this.toString() == binaryData.toString() },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = null,
              transactionEventCode = null,
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
    }

  @Test
  fun `Should write event to dead letter queue for error checkpointing event`() = runTest {
    // pre-condition
    val invalidEvent = "test"
    val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    given(checkpointer.success())
      .willReturn(Mono.error(RuntimeException("Error checkpointing event")))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(Mono.error(RuntimeException("Error writing event to queue")))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosePaymentQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
  }

  @Test
  fun `Should write event to dead letter queue for unhandled parsed event`() = runTest {
    val invalidEvent = "test"
    val payload = invalidEvent.toByteArray(StandardCharsets.UTF_8)
    val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
    // pre-condition

    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(transactionClosePaymentQueueConsumer.parseEvent(payload)).willReturn(Mono.just(mock()))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(transactionClosePaymentQueueConsumer.messageReceiver(payload, checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
  }
}
