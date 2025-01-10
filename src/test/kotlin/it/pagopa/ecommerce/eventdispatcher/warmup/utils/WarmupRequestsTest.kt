package it.pagopa.ecommerce.eventdispatcher.warmup.utils

import com.azure.core.util.BinaryData
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationOutcomeWaitingEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent as TransactionAuthorizationRequestedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureErrorEvent as TransactionClosureErrorEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRequestedEvent as TransactionClosureRequestedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRetriedEvent as TransactionClosureRetriedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionExpiredEvent as TransactionExpiredEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserCanceledEvent as TransactionUserCanceledEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptRequestedEvent as TransactionUserReceiptRequestedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.validation.BeanValidationConfiguration
import it.pagopa.ecommerce.payment.requests.warmup.utils.WarmupRequests
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@Import(BeanValidationConfiguration::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class WarmupRequestsTest {

  private val objectMapper = ObjectMapper()
  private val queuesConsumerConfig = QueuesConsumerConfig()
  val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()

  fun parseEvent(payload: ByteArray): Mono<TransactionEvent<*>> {
    val data = BinaryData.fromBytes(payload)
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()

    val events =
      listOf(
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEventV2>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionUserCanceledEventV2>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionClosureRequestedEventV2>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionClosureRetriedEventV2>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionClosureErrorEventV2>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionUserReceiptRequestedEventV2>>() {},
              jsonSerializerV2)
           ,
          data
            .toObjectAsync(
              object : TypeReference<QueueEvent<TransactionRefundRetriedEventV2>>() {},
              jsonSerializerV2)
           )
        .map { it.onErrorResume { Mono.empty() } }

    return Mono.firstWithValue(events).onErrorMap { InvalidEventException(data.toBytes(), it) }
  }
  
  @Test
  fun `test parse TransactionAuthorizationOutcomeWaitingEvent`() {
    val payload = WarmupRequests.getTransactionAuthorizationOutcomeWaitingEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionAuthorizationOutcomeWaitingEvent::class.java, event::class.java)
        assertEquals("TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT", event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionAuthorizationRequestedEvent`() {
    val payload = WarmupRequests.getTransactionAuthorizationRequestedEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionAuthorizationRequestedEventV2::class.java, event::class.java)
        assertEquals("TRANSACTION_AUTHORIZATION_REQUESTED_EVENT", event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionClosureRequestedEvent`() {
    val payload = WarmupRequests.getTransactionClosureRequestedEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionClosureRequestedEventV2::class.java, event::class.java)
        assertEquals("TRANSACTION_CLOSURE_REQUESTED_EVENT", event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionClosureErrorEvent`() {
    val payload = WarmupRequests.getTransactionClosureErrorEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionClosureErrorEventV2::class.java, event::class.java)
        assertEquals("TRANSACTION_CLOSURE_ERROR_EVENT", event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionExpiredEvent`() {
    val payload = WarmupRequests.getTransactionExpiredEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionExpiredEventV2::class.java, event::class.java)
        assertEquals("TRANSACTION_EXPIRED_EVENT", event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionUserReceiptRequestedEvent`() {
    val payload = WarmupRequests.getTransactionUserReceiptRequestedEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionUserReceiptRequestedEventV2::class.java, event::class.java)
        assertEquals("TRANSACTION_USER_RECEIPT_REQUESTED_EVENT", event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionRefundRetriedEvent`() {
    val payload = WarmupRequests.getTransactionRefundRetriedEvent()
    val result = parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionRefundRetriedEventV2::class.java, event::class.java)
        assertEquals("TRANSACTION_REFUND_RETRIED_EVENT", event.eventCode)
      }
      .verifyComplete()
  }
}
