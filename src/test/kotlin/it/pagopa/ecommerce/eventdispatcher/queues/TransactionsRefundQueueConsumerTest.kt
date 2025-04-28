package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRequestedEvent as TransactionRefundRequestedEventV1V2
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils as TransactionTestUtilsV2
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
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

@ExperimentalCoroutinesApi
class TransactionsRefundQueueConsumerTest {

  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionsRefundQueueConsumer =
    mock()
  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val queueConsumerV2Captor:
    KArgumentCaptor<
      Either<
        QueueEvent<TransactionRefundRetriedEventV2>,
        QueueEvent<TransactionRefundRequestedEventV1V2>>> =
    argumentCaptor<
      Either<
        QueueEvent<TransactionRefundRetriedEventV2>,
        QueueEvent<TransactionRefundRequestedEventV1V2>>>()

  private val checkpointer: Checkpointer = mock()

  private val queueConsumer =
    spy(
      TransactionsRefundQueueConsumer(
        queueConsumerV2 = queueConsumerV2,
        deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
        strictSerializerProviderV1 = strictSerializerProviderV1,
        strictSerializerProviderV2 = strictSerializerProviderV2))

  companion object {
    private val queuesConsumerConfig = QueuesConsumerConfig()

    val strictSerializerProviderV1 = queuesConsumerConfig.strictSerializerProviderV1()
    val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()
    private val refundRequestedV2 =
      TransactionTestUtilsV2.transactionRefundRequestedEvent(
        TransactionTestUtilsV2.transactionActivated(ZonedDateTime.now().toString()))
    private val refundRetriedV2 = TransactionTestUtilsV2.transactionRefundRetriedEvent(1)

    private val tracingInfo = TracingInfoTest.MOCK_TRACING_INFO

    @JvmStatic
    fun eventToHandleTestV2(): Stream<Arguments> {

      return Stream.of(
        Arguments.of(
          String(
            BinaryData.fromObject(
                QueueEvent(refundRequestedV2, tracingInfo),
                strictSerializerProviderV2.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          refundRequestedV2),
        Arguments.of(
          String(
            BinaryData.fromObject(
                QueueEvent(refundRetriedV2, tracingInfo),
                strictSerializerProviderV2.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          refundRetriedV2),
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
        queueConsumer.messageReceiver(
          serializedEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .verifyComplete()
    // assertions
    verify(queueConsumerV2, times(1)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any())
    val queueEvent = queueConsumerV2Captor.firstValue
    val event = queueEvent.fold({ it.event }, { it.event })
    val tracingInfo = queueEvent.fold({ it.tracingInfo }, { it.tracingInfo })
    assertEquals(originalEvent, event)
    assertNotNull(tracingInfo)
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
        queueConsumer.messageReceiver(
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
          queueConsumer.messageReceiver(
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
        queueConsumer.messageReceiver(
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
    given(queueConsumer.parseEvent(payload)).willReturn(Mono.just(mock()))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(queueConsumer.messageReceiver(payload, checkpointer))
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
