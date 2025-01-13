package it.pagopa.ecommerce.payment.requests.warmup.utils

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import java.time.ZonedDateTime
import org.springframework.http.HttpStatus
import reactor.core.publisher.Mono

object DummyCheckpointer : Checkpointer {
  override fun success(): Mono<Void> = Mono.empty()
  override fun failure(): Mono<Void> = Mono.empty()
}

object WarmupRequests {
  private val queuesConsumerConfig = QueuesConsumerConfig()
  val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()

  fun getTransactionAuthorizationOutcomeWaitingEventObject():
    TransactionAuthorizationOutcomeWaitingEvent {
    val event = TransactionAuthorizationOutcomeWaitingEvent()
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()

    val data = TransactionRetriedData(1)
    event.data = data
    event.eventCode =
      TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT.toString()
    return event
  }

  fun getTransactionAuthorizationOutcomeWaitingEvent(): ByteArray {
    val event = getTransactionAuthorizationOutcomeWaitingEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionUserReceiptAddErrorEventObject(): TransactionUserReceiptAddErrorEvent {
    val event = TransactionUserReceiptAddErrorEvent()
    event.id = "12345678-1234-1234-1234-123456789012"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-13T10:26:33.000Z[Etc/UTC]").toString()

    val data = TransactionUserReceiptData()
    data.language = "en"
    data.paymentDate = ZonedDateTime.parse("2025-01-12T10:00:00.000Z").toString()
    data.responseOutcome = TransactionUserReceiptData.Outcome.OK

    event.data = data
    event.eventCode = TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT.toString()
    return event
  }

  fun getTransactionUserReceiptAddErrorEvent(): ByteArray {
    val event = getTransactionUserReceiptAddErrorEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionAuthorizationRequestedEventObject(): TransactionAuthorizationRequestedEvent {
    val event = TransactionAuthorizationRequestedEvent()
    event.id = "7ee814b9-8bb8-4f61-9204-2aa55cb56773"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()
    event.data = null
    event.eventCode = TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()
    return event
  }

  fun getTransactionAuthorizationRequestedEvent(): ByteArray {
    val event = getTransactionAuthorizationRequestedEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionClosureRequestedEventObject(): TransactionClosureRequestedEvent {
    val event = TransactionClosureRequestedEvent()
    event.id = "7ee814b9-8bb8-4f61-9204-2aa55cb56773"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()
    event.eventCode = TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()
    return event
  }

  fun getTransactionClosureRequestedEvent(): ByteArray {
    val event = getTransactionClosureRequestedEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionClosureErrorEventObject(): TransactionClosureErrorEvent {
    val event = TransactionClosureErrorEvent()
    event.id = "7ee814b9-8bb8-4f61-9204-2aa55cb56773"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()

    val data = ClosureErrorData()
    data.httpErrorCode = HttpStatus.INTERNAL_SERVER_ERROR
    data.errorDescription = "Sample error message"
    data.errorType = ClosureErrorData.ErrorType.KO_RESPONSE_RECEIVED

    event.data = data
    event.eventCode = TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString()
    return event
  }

  fun getTransactionClosureErrorEvent(): ByteArray {
    val event = getTransactionClosureErrorEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionExpiredEventObject(): TransactionExpiredEvent {
    val event = TransactionExpiredEvent()
    event.id = "7ee814b9-8bb8-4f61-9204-2aa55cb56773"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()

    val data = TransactionExpiredData()
    data.statusBeforeExpiration =
      it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto.REFUND_REQUESTED

    event.data = data
    event.eventCode = TransactionEventCode.TRANSACTION_EXPIRED_EVENT.toString()
    return event
  }

  fun getTransactionExpiredEvent(): ByteArray {
    val event = getTransactionExpiredEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionUserReceiptRequestedEventObject(): TransactionUserReceiptRequestedEvent {
    val event = TransactionUserReceiptRequestedEvent()
    event.id = "7ee814b9-8bb8-4f61-9204-2aa55cb56773"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()

    val data = TransactionUserReceiptData()
    data.responseOutcome = TransactionUserReceiptData.Outcome.OK
    data.language = "en"
    data.paymentDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()

    event.data = data
    event.eventCode = TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()
    return event
  }

  fun getTransactionUserReceiptRequestedEvent(): ByteArray {
    val event = getTransactionUserReceiptRequestedEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionRefundRetriedEventObject(): TransactionRefundRetriedEvent {
    val event = TransactionRefundRetriedEvent()
    event.id = "7ee814b9-8bb8-4f61-9204-2aa55cb56773"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-10T14:28:47.843515440Z[Etc/UTC]").toString()

    val data = TransactionRefundRetriedData()
    data.transactionGatewayAuthorizationData = null
    data.retryCount = 1

    event.data = data
    event.eventCode = TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT.toString()
    return event
  }

  fun getTransactionRefundRetriedEvent(): ByteArray {
    val event = getTransactionRefundRetriedEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }

  fun getTransactionRefundRequestedEventObject(): TransactionRefundRequestedEvent {
    val event = TransactionRefundRequestedEvent()
    event.id = "abcdef12-3456-7890-abcd-ef1234567890"
    event.transactionId = "00000000000000000000000000000000"
    event.creationDate = ZonedDateTime.parse("2025-01-13T10:30:00.000Z[Etc/UTC]").toString()

    val data = TransactionRefundRequestedData()
    data.gatewayAuthData = null
    data.statusBeforeRefunded =
      it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
        .AUTHORIZATION_REQUESTED

    event.data = data
    event.eventCode = TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT.toString()
    return event
  }

  fun getTransactionRefundRequestedEvent(): ByteArray {
    val event = getTransactionRefundRequestedEventObject()
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.getObjectMapper()
    return objectMapper.writeValueAsBytes(queueEvent)
  }
}
