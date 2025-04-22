package it.pagopa.ecommerce.eventdispatcher.utils

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionRefunded
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import java.time.Duration
import java.time.ZonedDateTime
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
class TransactionTracing(@Autowired private val openTelemetryUtils: OpenTelemetryUtils) {
  companion object {
    const val TRANSACTIONID: String = "eCommerce.transactionId"
    const val TRANSACTIONSTATUS: String = "eCommerce.transactionStatus"
    const val PSPID: String = "eCommerce.pspId"
    const val CLIENTID: String = "eCommerce.clientId"
    const val PAYMENTMETHOD: String = "eCommerce.paymentMethod"

    // From activated datetime to the final status datetime, in milliseconds
    const val TRANSACTIONTOTALTIME: String = "eCommerce.transactionLifecycleTime"

    // From authorization requested datetime to authorization completed, in milliseconds
    const val TRANSACTIONAUTHORIZATIONTIME: String = "eCommerce.transactionAuthorizationProcessTime"

    // From close payment request to add user receipt response, in milliseconds
    const val TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME: String =
      "eCommerce.transactionClosePaymentToUserReceiptTime"
  }

  private val logger = LoggerFactory.getLogger(javaClass)

  fun addSpanAttributesNotificationsFlowFromTransaction(
    tx: BaseTransactionWithUserReceipt,
    events: Flux<TransactionEvent<Any>>,
  ) {
    events
      .collectMap({ event -> event.eventCode }, { event -> event.creationDate })
      .map { eventDateMap ->
        // Calculate durations in milliseconds
        val authorizationDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT.toString()])

        val closePaymentToAddUserReceiptRequestedDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()])

        val totalDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()])

        val builder =
          Attributes.builder()
            .put(AttributeKey.stringKey(TRANSACTIONID), tx.transactionId.value())
            .put(AttributeKey.stringKey(TRANSACTIONSTATUS), tx.status.value)
            .put(AttributeKey.stringKey(CLIENTID), tx.clientId.toString())
            .put(AttributeKey.stringKey(PSPID), tx.transactionAuthorizationRequestData.pspId)
            .put(
              AttributeKey.stringKey(PAYMENTMETHOD),
              tx.transactionAuthorizationRequestData.paymentMethodName)

        // Only add duration attributes if they have valid values
        if (totalDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONTOTALTIME), totalDuration)
        }

        if (authorizationDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONAUTHORIZATIONTIME), authorizationDuration)
        }

        if (closePaymentToAddUserReceiptRequestedDuration != null) {
          builder.put(
            AttributeKey.longKey(TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME),
            closePaymentToAddUserReceiptRequestedDuration)
        }
        builder.build()
      }
      .doOnSuccess { attributes ->
        openTelemetryUtils.addSpanWithAttributes(TransactionTracing::class.simpleName, attributes)
      }
      .doOnError { error ->
        logger.warn("Failed to extract span attributes: ${error.message}", error)
      }
      // Fire and forget
      .subscribe(
        {}, { error -> logger.warn("Unhandled error in span attributes extraction", error) })
  }

  fun addSpanAttributesExpiredFlowFromTransaction(
    tx: BaseTransaction,
    events: Flux<TransactionEvent<Any>>,
  ) {
    val isFinalStatus =
      tx.status == TransactionStatusDto.EXPIRED_NOT_AUTHORIZED ||
        tx.status == TransactionStatusDto.CANCELLATION_EXPIRED

    if (!isFinalStatus) {
      return
    }

    events
      .collectMap({ event -> event.eventCode }, { event -> event.creationDate })
      .map { eventDateMap ->
        // Calculate durations in milliseconds
        val authorizationDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT.toString()])

        val closePaymentToAddUserReceiptRequestedDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()])

        val totalDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()])

        val builder =
          Attributes.builder()
            .put(AttributeKey.stringKey(TRANSACTIONID), tx.transactionId.value())
            .put(AttributeKey.stringKey(TRANSACTIONSTATUS), tx.status.value)
            .put(AttributeKey.stringKey(CLIENTID), tx.clientId.toString())

        if (tx is BaseTransactionWithRequestedAuthorization) {
          builder
            .put(AttributeKey.stringKey(PSPID), tx.transactionAuthorizationRequestData.pspId)
            .put(
              AttributeKey.stringKey(PAYMENTMETHOD),
              tx.transactionAuthorizationRequestData.paymentMethodName)
        }

        if (totalDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONTOTALTIME), totalDuration)
        }
        if (authorizationDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONAUTHORIZATIONTIME), authorizationDuration)
        }
        if (closePaymentToAddUserReceiptRequestedDuration != null) {
          builder.put(
            AttributeKey.longKey(TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME),
            closePaymentToAddUserReceiptRequestedDuration)
        }

        builder.build()
      }
      .doOnSuccess { attributes ->
        openTelemetryUtils.addSpanWithAttributes(TransactionTracing::class.simpleName, attributes)
      }
      .doOnError { error ->
        logger.warn("Failed to extract span attributes: ${error.message}", error)
      }
      .subscribe(
        {}, { error -> logger.warn("Unhandled error in span attributes extraction", error) })
  }

  fun addSpanAttributesRefundedFlowFromTransaction(
    tx: BaseTransaction,
    events: Flux<BaseTransactionEvent<Any>>
  ) {
    if (tx.status != TransactionStatusDto.REFUNDED) {
      return
    }

    events
      .collectMap({ event -> event.eventCode }, { event -> event.creationDate })
      .map { eventDateMap ->
        // Calculate durations in milliseconds
        val authorizationDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT.toString()])

        val closePaymentToAddUserReceiptRequestedDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()])

        val totalDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()])

        val builder =
          Attributes.builder()
            .put(AttributeKey.stringKey(TRANSACTIONID), tx.transactionId.value())
            .put(AttributeKey.stringKey(TRANSACTIONSTATUS), tx.status.value)
            .put(AttributeKey.stringKey(CLIENTID), tx.clientId.toString())

        if (tx is BaseTransactionRefunded) {
          builder
            .put(AttributeKey.stringKey(PSPID), tx.transactionAuthorizationRequestData.pspId)
            .put(
              AttributeKey.stringKey(PAYMENTMETHOD),
              tx.transactionAuthorizationRequestData.paymentMethodName)
        }

        if (totalDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONTOTALTIME), totalDuration)
        }

        if (authorizationDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONAUTHORIZATIONTIME), authorizationDuration)
        }

        if (closePaymentToAddUserReceiptRequestedDuration != null) {
          builder.put(
            AttributeKey.longKey(TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME),
            closePaymentToAddUserReceiptRequestedDuration)
        }

        val attributes = builder.build()

        println("OpenTelemetry Attributes:")
        attributes.forEach { key, value ->
          println("Key: ${key.key}, Type: ${key.type}, Value: $value")
        }

        attributes
      }
      .doOnSuccess { attributes ->
        openTelemetryUtils.addSpanWithAttributes(TransactionTracing::class.simpleName, attributes)
      }
      .doOnError { error ->
        logger.warn("Failed to extract span attributes: ${error.message}", error)
      }
      .subscribe(
        {}, { error -> logger.warn("Unhandled error in span attributes extraction", error) })
  }

  fun addSpanAttributesCanceledFlowFromTransaction(
    tx: BaseTransaction,
    events: Flux<TransactionEvent<Any>>
  ) {
    if (tx.status != TransactionStatusDto.CANCELED) {
      return
    }

    events
      .collectMap({ event -> event.eventCode }, { event -> event.creationDate })
      .map { eventDateMap ->
        // Calculate durations in milliseconds
        val authorizationDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT.toString()])

        val closePaymentToAddUserReceiptRequestedDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()])

        val totalDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()])

        val builder =
          Attributes.builder()
            .put(AttributeKey.stringKey(TRANSACTIONID), tx.transactionId.value())
            .put(AttributeKey.stringKey(TRANSACTIONSTATUS), tx.status.value)
            .put(AttributeKey.stringKey(CLIENTID), tx.clientId.toString())

        if (tx is BaseTransactionRefunded) {
          builder
            .put(AttributeKey.stringKey(PSPID), tx.transactionAuthorizationRequestData.pspId)
            .put(
              AttributeKey.stringKey(PAYMENTMETHOD),
              tx.transactionAuthorizationRequestData.paymentMethodName)
        }

        if (totalDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONTOTALTIME), totalDuration)
        }

        if (authorizationDuration != null) {
          builder.put(AttributeKey.longKey(TRANSACTIONAUTHORIZATIONTIME), authorizationDuration)
        }

        if (closePaymentToAddUserReceiptRequestedDuration != null) {
          builder.put(
            AttributeKey.longKey(TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME),
            closePaymentToAddUserReceiptRequestedDuration)
        }

        val attributes = builder.build()

        println("OpenTelemetry Attributes:")
        attributes.forEach { key, value ->
          println("Key: ${key.key}, Type: ${key.type}, Value: $value")
        }

        attributes
      }
      .doOnSuccess { attributes ->
        openTelemetryUtils.addSpanWithAttributes(TransactionTracing::class.simpleName, attributes)
      }
      .doOnError { error ->
        logger.warn("Failed to extract span attributes: ${error.message}", error)
      }
      .subscribe(
        {}, { error -> logger.warn("Unhandled error in span attributes extraction", error) })
  }

  /**
   * Calculates the duration in milliseconds between two date strings.
   *
   * @param startDateString The start date as a string in ISO-8601 format, can be null
   * @param endDateString The end date as a string in ISO-8601 format, can be null
   * @return Duration in milliseconds, or 0 if either date is invalid or null
   */
  private fun calculateDurationMs(startDateString: String?, endDateString: String?): Long? {
    val startDate = parseDate(startDateString)
    val endDate = parseDate(endDateString)

    if (startDate == null || endDate == null) return null
    return Duration.between(startDate, endDate).toMillis()
  }

  /**
   * Helper method to parse a date string to ZonedDateTime.
   *
   * @param dateString The date string in ISO-8601 format
   * @return Parsed ZonedDateTime or null if the string is null, empty, or invalid
   */
  private fun parseDate(dateString: String?): ZonedDateTime? {
    if (dateString.isNullOrEmpty()) return null
    return try {
      ZonedDateTime.parse(dateString)
    } catch (e: Exception) {
      null
    }
  }
}
