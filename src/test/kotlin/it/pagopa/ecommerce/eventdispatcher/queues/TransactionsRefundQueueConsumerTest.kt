package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRequestedEvent as TransactionRefundRequestedEventV1
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRequestedEvent as TransactionRefundRequestedEventV1V2
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils as TransactionTestUtilsV1
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils as TransactionTestUtilsV2
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.ZonedDateTime
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
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

  private val queueConsumerV1:
    it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionsRefundQueueConsumer =
    mock()

  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionsRefundQueueConsumer =
    mock()
  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  private val deadLetterTTLSeconds = 1

  private val queueConsumerV1Captor:
    KArgumentCaptor<
      Pair<
        Either<TransactionRefundRetriedEventV1, TransactionRefundRequestedEventV1>, TracingInfo?>> =
    argumentCaptor<
      Pair<
        Either<TransactionRefundRetriedEventV1, TransactionRefundRequestedEventV1>, TracingInfo?>>()

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

  private val transactionClosePaymentQueueConsumer =
    spy(
      TransactionsRefundQueueConsumer(
        queueConsumerV1 = queueConsumerV1,
        queueConsumerV2 = queueConsumerV2,
        deadLetterQueueAsyncClient = deadLetterQueueAsyncClient,
        deadLetterTTLSeconds = deadLetterTTLSeconds,
        strictSerializerProviderV1 = strictSerializerProviderV1,
        strictSerializerProviderV2 = strictSerializerProviderV2))

  companion object {
    private val queuesConsumerConfig = QueuesConsumerConfig()

    val strictSerializerProviderV1 = queuesConsumerConfig.strictSerializerProviderV1()
    val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()
    private val refundRequestedV1 =
      TransactionTestUtilsV1.transactionRefundRequestedEvent(
        TransactionTestUtilsV1.transactionActivated(ZonedDateTime.now().toString()))
    private val refundRequestedV2 =
      TransactionTestUtilsV2.transactionRefundRequestedEvent(
        TransactionTestUtilsV2.transactionActivated(ZonedDateTime.now().toString()))
    private val refundRetriedV1 = TransactionTestUtilsV1.transactionRefundRetriedEvent(1)
    private val refundRetriedV2 = TransactionTestUtilsV2.transactionRefundRetriedEvent(1)

    private val tracingInfo = TracingInfoTest.MOCK_TRACING_INFO

    @JvmStatic
    fun eventToHandleTestV1(): Stream<Arguments> {

      return Stream.of(
        Arguments.of(
          String(
            BinaryData.fromObject(refundRequestedV1, strictSerializerProviderV1.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          refundRequestedV1,
          false),
        Arguments.of(
          String(
            BinaryData.fromObject(
                QueueEvent(refundRequestedV1, tracingInfo),
                strictSerializerProviderV1.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          refundRequestedV1,
          true),
        Arguments.of(
          String(
            BinaryData.fromObject(refundRetriedV1, strictSerializerProviderV1.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          refundRetriedV1,
          false),
        Arguments.of(
          String(
            BinaryData.fromObject(
                QueueEvent(refundRetriedV1, tracingInfo),
                strictSerializerProviderV1.createInstance())
              .toBytes(),
            StandardCharsets.UTF_8),
          refundRetriedV1,
          true),
      )
    }

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
  @MethodSource("eventToHandleTestV1")
  fun `Should dispatch TransactionV1 events`(
    serializedEvent: String,
    originalEvent: BaseTransactionEvent<*>,
    withTracingInfo: Boolean
  ) = runTest {
    // pre-condition
    println("Serialized event: $serializedEvent")
    given(queueConsumerV1.messageReceiver(queueConsumerV1Captor.capture(), any()))
      .willReturn(Mono.empty())
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosePaymentQueueConsumer.messageReceiver(
          serializedEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .verifyComplete()
    // assertions
    verify(queueConsumerV1, times(1)).messageReceiver(any(), any())
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
    val (parsedEvent, tracingInfo) = queueConsumerV1Captor.firstValue
    val event = parsedEvent.fold({ it }, { it })
    assertEquals(originalEvent, event)
    if (withTracingInfo) {
      assertNotNull(tracingInfo)
    } else {
      assertNull(tracingInfo)
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
    verify(queueConsumerV1, times(0)).messageReceiver(any(), any())
    verify(queueConsumerV2, times(1)).messageReceiver(any(), any())
    verify(deadLetterQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
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
    given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosePaymentQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .verifyComplete()
    // assertions
    verify(queueConsumerV1, times(0)).messageReceiver(any(), any())
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
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
      given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(Mono.error(RuntimeException("Error writing event to queue")))
      // test
      Hooks.onOperatorDebug()
      StepVerifier.create(
          transactionClosePaymentQueueConsumer.messageReceiver(
            invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
        .expectError(java.lang.RuntimeException::class.java)
        .verify()
      // assertions
      verify(queueConsumerV1, times(0)).messageReceiver(any(), any())
      verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
      verify(deadLetterQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> { this.toString() == binaryData.toString() },
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
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
    given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
      .willReturn(Mono.error(RuntimeException("Error writing event to queue")))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosePaymentQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()
    // assertions
    verify(queueConsumerV1, times(0)).messageReceiver(any(), any())
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
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
    given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    given(transactionClosePaymentQueueConsumer.parseEvent(payload)).willReturn(Mono.just(mock()))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(transactionClosePaymentQueueConsumer.messageReceiver(payload, checkpointer))
      .verifyComplete()
    // assertions
    verify(queueConsumerV1, times(0)).messageReceiver(any(), any())
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
  }
}
