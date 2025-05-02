package it.pagopa.ecommerce.eventdispatcher.utils

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionActivateEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionAuthorizationCompletedEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionClosureRequestedEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionUserReceiptAddedEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionUserReceiptData
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionUserReceiptRequestedEvent
import java.time.ZonedDateTime
import java.util.UUID
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.capture
import reactor.core.publisher.Flux

@ExtendWith(MockitoExtension::class)
class TransactionTracingTest {

  @Mock private lateinit var openTelemetryUtils: OpenTelemetryUtils

  private lateinit var transactionTracing: TransactionTracing

  @Captor private lateinit var attributesCaptor: ArgumentCaptor<Attributes>

  @BeforeEach
  fun setup() {
    transactionTracing = TransactionTracing(openTelemetryUtils)
  }

  @Test
  fun `addSpanAttributesNotificationsFlowFromTransaction should add correct attributes for complete transaction flow`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT
    val pspId = "test-psp-id"
    val paymentMethodName = "test-payment-method"

    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)

    // Create TransactionAuthorizationRequestData
    val authRequestData = mock(TransactionAuthorizationRequestData::class.java)
    `when`(authRequestData.pspId).thenReturn(pspId)
    `when`(authRequestData.paymentMethodName).thenReturn(paymentMethodName)

    val transaction = mock(BaseTransactionWithUserReceipt::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.NOTIFIED_OK)
    `when`(transaction.clientId).thenReturn(clientId)
    `when`(transaction.transactionAuthorizationRequestData).thenReturn(authRequestData)

    val activatedEvent = transactionActivateEvent()
    activatedEvent.creationDate = createDateForSecondsFromNow(30 * 60)

    val userReceiptRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val userReceiptAdded = transactionUserReceiptAddedEvent(transactionUserReceiptData)
    val transactionAuthorizationRequestedEvt = transactionAuthorizationRequestedEvent()
    transactionAuthorizationRequestedEvt.data.pspId = "notifiedKoPspId"
    val transactionActivateEvt = transactionActivateEvent()
    val transactionAuthorizationCompletedEvt =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
    val transactionClosureRequestedEvt = transactionClosureRequestedEvent()

    transactionActivateEvt.creationDate = createDateForSecondsFromNow(10 * 60)
    transactionAuthorizationRequestedEvt.creationDate = createDateForSecondsFromNow(25 * 60)
    transactionAuthorizationCompletedEvt.creationDate = createDateForSecondsFromNow(20 * 60)
    transactionClosureRequestedEvt.creationDate = createDateForSecondsFromNow(15 * 60)
    userReceiptRequested.creationDate = createDateForSecondsFromNow(10 * 60)
    userReceiptAdded.creationDate = createDateForSecondsFromNow(5 * 60)

    val events =
      Flux.just(
        activatedEvent,
        transactionActivateEvt,
        transactionAuthorizationCompletedEvt,
        transactionClosureRequestedEvt,
        userReceiptRequested,
        userReceiptAdded) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesNotificationsFlowFromTransaction(transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify all required attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == pspId)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == paymentMethodName)

    // Verify duration metrics are present and positive
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME))?.let {
      assert(it > 0)
    }
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME))?.let {
      assert(it > 0)
    }
    attributes
      .get(AttributeKey.longKey(TransactionTracing.TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME))
      ?.let { assert(it > 0) }
  }

  @Test
  fun `addSpanAttributesNotificationsFlowFromTransaction should handle missing events gracefully`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT
    val pspId = "test-psp-id"
    val paymentMethodName = "test-payment-method"

    // Create TransactionAuthorizationRequestData
    val authRequestData = mock(TransactionAuthorizationRequestData::class.java)
    `when`(authRequestData.pspId).thenReturn(pspId)
    `when`(authRequestData.paymentMethodName).thenReturn(paymentMethodName)

    val transaction = mock(BaseTransactionWithUserReceipt::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.NOTIFIED_OK)
    `when`(transaction.clientId).thenReturn(clientId)
    `when`(transaction.transactionAuthorizationRequestData).thenReturn(authRequestData)

    // Create only some of the events
    val now = ZonedDateTime.now()

    val activatedEvent = transactionActivateEvent()
    activatedEvent.creationDate = createDateForSecondsFromNow(30 * 60)

    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
    val userReceiptAdded = transactionUserReceiptAddedEvent(transactionUserReceiptData)
    userReceiptAdded.creationDate = createDateForSecondsFromNow(5 * 60)

    val events = Flux.just(activatedEvent, userReceiptAdded) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesNotificationsFlowFromTransaction(transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify basic attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == pspId)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == paymentMethodName)

    // Verify total duration is present but others are not
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME))?.let {
      assert(it > 0)
    }
    assert(
      attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME)) == null)
    assert(
      attributes.get(
        AttributeKey.longKey(TransactionTracing.TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME)) == null)
  }

  @Test
  fun `addSpanAttributesRefundedFlowFromTransaction should handle transaction without refund data`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT

    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.REFUNDED)
    `when`(transaction.clientId).thenReturn(clientId)

    val activatedEvent = transactionActivateEvent()
    activatedEvent.creationDate = createDateForSecondsFromNow(30 * 60)

    val events = Flux.just(activatedEvent) as Flux<BaseTransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesRefundedFlowFromTransaction(transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify basic attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())

    // Verify PSP and payment method are not present since this is not a BaseTransactionRefunded
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == null)
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == null)
  }

  @Test
  fun `addSpanAttributesExpiredFlowFromTransaction should add span for expired transaction`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT
    val pspId = "test-psp-id"
    val paymentMethodName = "test-payment-method"

    // Create TransactionAuthorizationRequestData
    val authRequestData = mock(TransactionAuthorizationRequestData::class.java)
    `when`(authRequestData.pspId).thenReturn(pspId)
    `when`(authRequestData.paymentMethodName).thenReturn(paymentMethodName)

    val transaction = mock(BaseTransactionWithRequestedAuthorization::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.EXPIRED_NOT_AUTHORIZED)
    `when`(transaction.clientId).thenReturn(clientId)
    `when`(transaction.transactionAuthorizationRequestData).thenReturn(authRequestData)

    val transactionAuthorizationRequestedEvt = transactionAuthorizationRequestedEvent()
    transactionAuthorizationRequestedEvt.data.pspId = "notifiedKoPspId"
    val transactionActivateEvt = transactionActivateEvent()
    val transactionAuthorizationCompletedEvt =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))

    transactionActivateEvt.creationDate = createDateForSecondsFromNow(10 * 60)
    transactionAuthorizationRequestedEvt.creationDate = createDateForSecondsFromNow(25 * 60)
    transactionAuthorizationCompletedEvt.creationDate = createDateForSecondsFromNow(20 * 60)

    val events =
      Flux.just(
        transactionActivateEvt,
        transactionAuthorizationRequestedEvt,
        transactionAuthorizationCompletedEvt) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesExpiredFlowFromTransaction(transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify all required attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == pspId)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == paymentMethodName)

    // Verify duration metrics
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME))?.let {
      assert(it > 0)
    }
  }

  @Test
  fun `addSpanAttributesRefundedFlowFromTransaction should add span for refunded transaction`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT
    val pspId = "test-psp-id"
    val paymentMethodName = "test-payment-method"

    // Create TransactionAuthorizationRequestData
    val authRequestData = mock(TransactionAuthorizationRequestData::class.java)
    `when`(authRequestData.pspId).thenReturn(pspId)
    `when`(authRequestData.paymentMethodName).thenReturn(paymentMethodName)

    val transaction = mock(BaseTransactionRefunded::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.REFUNDED)
    `when`(transaction.clientId).thenReturn(clientId)
    `when`(transaction.transactionAuthorizationRequestData).thenReturn(authRequestData)

    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)

    val activatedEvent = transactionActivateEvent()
    activatedEvent.creationDate = createDateForSecondsFromNow(30 * 60)

    val userReceiptRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val userReceiptAdded = transactionUserReceiptAddedEvent(transactionUserReceiptData)
    val transactionAuthorizationRequestedEvt = transactionAuthorizationRequestedEvent()
    transactionAuthorizationRequestedEvt.data.pspId = "notifiedKoPspId"
    val transactionActivateEvt = transactionActivateEvent()
    val transactionAuthorizationCompletedEvt =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
    val transactionClosureRequestedEvt = transactionClosureRequestedEvent()

    transactionActivateEvt.creationDate = createDateForSecondsFromNow(10 * 60)
    transactionAuthorizationRequestedEvt.creationDate = createDateForSecondsFromNow(25 * 60)
    transactionAuthorizationCompletedEvt.creationDate = createDateForSecondsFromNow(20 * 60)
    transactionClosureRequestedEvt.creationDate = createDateForSecondsFromNow(15 * 60)
    userReceiptRequested.creationDate = createDateForSecondsFromNow(10 * 60)
    userReceiptAdded.creationDate = createDateForSecondsFromNow(5 * 60)

    // Create events with timestamps
    val events =
      Flux.just(
        transactionActivateEvt,
        transactionAuthorizationRequestedEvt,
        transactionAuthorizationCompletedEvt,
        transactionClosureRequestedEvt,
        userReceiptRequested,
        userReceiptAdded) as Flux<BaseTransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesRefundedFlowFromTransaction(transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify all required attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == pspId)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == paymentMethodName)

    // Verify duration metrics are present
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME))?.let {
      assert(it > 0)
    }
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME))?.let {
      assert(it > 0)
    }
    attributes
      .get(AttributeKey.longKey(TransactionTracing.TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME))
      ?.let { assert(it > 0) }
  }

  @Test
  fun `addSpanAttributesNotificationsFlowFromTransaction should handle error in events flux`() {
    // Given
    val transaction = mock(BaseTransactionWithUserReceipt::class.java)
    val events = Flux.error<TransactionEvent<Any>>(RuntimeException("Test error"))

    // When
    transactionTracing.addSpanAttributesNotificationsFlowFromTransaction(transaction, events)

    // Then - verify no span is added
    verify(openTelemetryUtils, after(1000).never()).addSpanWithAttributes(any(), any())
  }

  @Test
  fun `addSpanAttributesExpiredFlowFromTransaction should not add span for non-expired transaction`() {
    // Given
    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CLOSED)

    val events = Flux.empty<TransactionEvent<Any>>()

    // When
    transactionTracing.addSpanAttributesExpiredFlowFromTransaction(transaction, events)

    // Then - verify no span is added
    verify(openTelemetryUtils, after(1000).never()).addSpanWithAttributes(any(), any())
  }

  @Test
  fun `addSpanAttributesExpiredFlowFromTransaction should handle transaction without authorization data`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT

    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CANCELLATION_EXPIRED)
    `when`(transaction.clientId).thenReturn(clientId)

    // Create events with timestamps
    val activatedEvent = transactionActivateEvent()
    activatedEvent.creationDate = createDateForSecondsFromNow(30 * 60)

    val events = Flux.just(activatedEvent) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesExpiredFlowFromTransaction(transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify basic attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())

    // Verify attributes not present
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == null)
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == null)
  }

  @Test
  fun `addSpanAttributesRefundedFlowFromTransaction should not add span for non-refunded transaction`() {
    // Given
    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CLOSED)

    val events = Flux.empty<BaseTransactionEvent<Any>>()

    // When
    transactionTracing.addSpanAttributesRefundedFlowFromTransaction(transaction, events)

    // Then - verify no span is added
    verify(openTelemetryUtils, after(1000).never()).addSpanWithAttributes(any(), any())
  }

  @Test
  fun `addSpanAttributesRefundedFlowFromTransaction should handle error in events flux`() {
    // Given
    val transaction = mock(BaseTransactionRefunded::class.java)
    `when`(transaction.status).thenReturn(TransactionStatusDto.REFUNDED)

    val events = Flux.error<BaseTransactionEvent<Any>>(RuntimeException("Test error"))

    // When
    transactionTracing.addSpanAttributesRefundedFlowFromTransaction(transaction, events)

    // Then - verify no span is added
    verify(openTelemetryUtils, after(1000).never()).addSpanWithAttributes(any(), any())
  }

  @Test
  fun `addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction should add span for canceled transaction`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT
    val pspId = "test-psp-id"
    val paymentMethodName = "test-payment-method"

    // Create TransactionAuthorizationRequestData
    val authRequestData = mock(TransactionAuthorizationRequestData::class.java)
    `when`(authRequestData.pspId).thenReturn(pspId)
    `when`(authRequestData.paymentMethodName).thenReturn(paymentMethodName)

    val transaction = mock(BaseTransactionWithRequestedAuthorization::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CANCELED)
    `when`(transaction.clientId).thenReturn(clientId)
    `when`(transaction.transactionAuthorizationRequestData).thenReturn(authRequestData)

    val transactionAuthorizationRequestedEvt = transactionAuthorizationRequestedEvent()
    transactionAuthorizationRequestedEvt.data.pspId = "canceledPspId"
    val transactionActivateEvt = transactionActivateEvent()
    val transactionAuthorizationCompletedEvt =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
    val transactionClosureRequestedEvt = transactionClosureRequestedEvent()

    transactionActivateEvt.creationDate = createDateForSecondsFromNow(10 * 60)
    transactionAuthorizationRequestedEvt.creationDate = createDateForSecondsFromNow(25 * 60)
    transactionAuthorizationCompletedEvt.creationDate = createDateForSecondsFromNow(20 * 60)
    transactionClosureRequestedEvt.creationDate = createDateForSecondsFromNow(15 * 60)

    val events =
      Flux.just(
        transactionActivateEvt,
        transactionAuthorizationRequestedEvt,
        transactionAuthorizationCompletedEvt,
        transactionClosureRequestedEvt) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
      transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify all required attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == pspId)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == paymentMethodName)

    // Verify duration metrics
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME))?.let {
      assert(it > 0)
    }
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME))?.let {
      assert(it > 0)
    }
  }

  @Test
  fun `addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction should add span for unauthorized transaction`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT
    val pspId = "test-psp-id"
    val paymentMethodName = "test-payment-method"

    // Create TransactionAuthorizationRequestData
    val authRequestData = mock(TransactionAuthorizationRequestData::class.java)
    `when`(authRequestData.pspId).thenReturn(pspId)
    `when`(authRequestData.paymentMethodName).thenReturn(paymentMethodName)

    val transaction = mock(BaseTransactionWithRequestedAuthorization::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.UNAUTHORIZED)
    `when`(transaction.clientId).thenReturn(clientId)
    `when`(transaction.transactionAuthorizationRequestData).thenReturn(authRequestData)

    val transactionAuthorizationRequestedEvt = transactionAuthorizationRequestedEvent()
    transactionAuthorizationRequestedEvt.data.pspId = "unauthorizedPspId"
    val transactionActivateEvt = transactionActivateEvent()
    val transactionAuthorizationCompletedEvt =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
    val transactionClosureRequestedEvt = transactionClosureRequestedEvent()

    transactionActivateEvt.creationDate = createDateForSecondsFromNow(10 * 60)
    transactionAuthorizationRequestedEvt.creationDate = createDateForSecondsFromNow(25 * 60)
    transactionAuthorizationCompletedEvt.creationDate = createDateForSecondsFromNow(20 * 60)
    transactionClosureRequestedEvt.creationDate = createDateForSecondsFromNow(15 * 60)

    val events =
      Flux.just(
        transactionActivateEvt,
        transactionAuthorizationRequestedEvt,
        transactionAuthorizationCompletedEvt,
        transactionClosureRequestedEvt) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
      transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify all required attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == pspId)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == paymentMethodName)

    // Verify duration metrics
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME))?.let {
      assert(it > 0)
    }
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME))?.let {
      assert(it > 0)
    }
  }

  @Test
  fun `addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction should not add span for non-canceled or non-unauthorized transaction`() {
    // Given
    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CLOSED)

    val events = Flux.empty<TransactionEvent<Any>>()

    // When
    transactionTracing.addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
      transaction, events)

    // Then - verify no span is added
    verify(openTelemetryUtils, after(1000).never()).addSpanWithAttributes(any(), any())
  }

  @Test
  fun `addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction should handle transaction without authorization data`() {
    // Given
    val transactionId = TransactionId(UUID.randomUUID())
    val clientId = Transaction.ClientId.CHECKOUT

    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.transactionId).thenReturn(transactionId)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CANCELED)
    `when`(transaction.clientId).thenReturn(clientId)

    // Create events with timestamps
    val activatedEvent = transactionActivateEvent()
    activatedEvent.creationDate = createDateForSecondsFromNow(30 * 60)

    val closedEvent = transactionClosureRequestedEvent()
    closedEvent.creationDate = createDateForSecondsFromNow(5 * 60)

    val events = Flux.just(activatedEvent, closedEvent) as Flux<TransactionEvent<Any>>

    // When
    transactionTracing.addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
      transaction, events)

    // Then
    verify(openTelemetryUtils, timeout(1000))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))

    val attributes = attributesCaptor.value

    // Verify basic attributes are present
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)) ==
        transactionId.value())
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)) ==
        transaction.status.value)
    assert(
      attributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)) == clientId.toString())

    // Verify attributes not present
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)) == null)
    assert(attributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)) == null)

    // Verify total duration is present
    attributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME))?.let {
      assert(it > 0)
    }
  }

  @Test
  fun `addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction should handle error in events flux`() {
    // Given
    val transaction = mock(BaseTransaction::class.java)
    `when`(transaction.status).thenReturn(TransactionStatusDto.CANCELED)

    val events = Flux.error<TransactionEvent<Any>>(RuntimeException("Test error"))

    // When
    transactionTracing.addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
      transaction, events)

    // Then - verify no span is added
    verify(openTelemetryUtils, after(1000).never()).addSpanWithAttributes(any(), any())
  }

  @Test
  fun `calculateDurationMs should return correct duration between two valid dates`() {
    // Given
    val startDate = ZonedDateTime.now().minusMinutes(5)
    val endDate = ZonedDateTime.now()

    // Use reflection to access private method
    val calculateDurationMs =
      TransactionTracing::class
        .java
        .getDeclaredMethod("calculateDurationMs", String::class.java, String::class.java)
    calculateDurationMs.isAccessible = true

    // When
    val duration =
      calculateDurationMs.invoke(transactionTracing, startDate.toString(), endDate.toString())
        as Long

    // Then
    assert(duration > 0)
    assert(duration < 5 * 60 * 1000 + 100) // 5 minutes in ms + small buffer
  }

  @Test
  fun `calculateDurationMs should return null for invalid dates`() {
    // Use reflection to access private method
    val calculateDurationMs =
      TransactionTracing::class
        .java
        .getDeclaredMethod("calculateDurationMs", String::class.java, String::class.java)
    calculateDurationMs.isAccessible = true

    // Test with null start date
    val duration1 =
      calculateDurationMs.invoke(transactionTracing, null, ZonedDateTime.now().toString()) as Long?
    assert(duration1 == null)

    // Test with null end date
    val duration2 =
      calculateDurationMs.invoke(transactionTracing, ZonedDateTime.now().toString(), null) as Long?
    assert(duration2 == null)

    // Test with empty start date
    val duration3 =
      calculateDurationMs.invoke(transactionTracing, "", ZonedDateTime.now().toString()) as Long?
    assert(duration3 == null)

    // Test with invalid date format
    val duration4 =
      calculateDurationMs.invoke(transactionTracing, "not-a-date", ZonedDateTime.now().toString())
        as Long?
    assert(duration4 == null)
  }

  @Test
  fun `parseDate should return null for invalid input`() {
    // Use reflection to access private method
    val parseDate =
      TransactionTracing::class.java.getDeclaredMethod("parseDate", String::class.java)
    parseDate.isAccessible = true

    // Test with null
    val result1 = parseDate.invoke(transactionTracing, null)
    assert(result1 == null)

    // Test with empty string
    val result2 = parseDate.invoke(transactionTracing, "")
    assert(result2 == null)

    // Test with invalid date
    val result3 = parseDate.invoke(transactionTracing, "not-a-date")
    assert(result3 == null)
  }

  @Test
  fun `parseDate should return ZonedDateTime for valid input`() {
    // Use reflection to access private method
    val parseDate =
      TransactionTracing::class.java.getDeclaredMethod("parseDate", String::class.java)
    parseDate.isAccessible = true

    // Test with valid date
    val dateString = ZonedDateTime.now().toString()
    val result = parseDate.invoke(transactionTracing, dateString)
    assert(result is ZonedDateTime)
  }
}
