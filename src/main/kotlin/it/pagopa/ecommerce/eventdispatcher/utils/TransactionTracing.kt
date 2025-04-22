package it.pagopa.ecommerce.eventdispatcher.utils

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import java.time.Duration
import java.time.ZonedDateTime
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
class TransactionTracing(private val openTelemetryUtils: OpenTelemetryUtils) {

  private val logger = LoggerFactory.getLogger(javaClass)

  companion object {
    // Attribute keys
    const val TRANSACTIONID = "eCommerce.transactionId"
    const val TRANSACTIONSTATUS = "eCommerce.transactionStatus"
    const val PSPID = "eCommerce.pspId"
    const val CLIENTID = "eCommerce.clientId"
    const val PAYMENTMETHOD = "eCommerce.paymentMethod"

    // Duration metrics
    // From activated datetime to the final status datetime, in milliseconds
    const val TRANSACTIONTOTALTIME = "eCommerce.transactionLifecycleTime"
    // From authorization requested datetime to authorization completed, in milliseconds
    const val TRANSACTIONAUTHORIZATIONTIME = "eCommerce.transactionAuthorizationProcessTime"
    // From close payment request to add user receipt response, in milliseconds
    const val TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME =
      "eCommerce.transactionClosePaymentToUserReceiptTime"

    // Event code pairs for duration calculation
    private val TOTAL_DURATION_EVENTS =
      mapOf(
        TransactionStatusDto.NOTIFIED_OK to
          Pair(
            TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()),
        TransactionStatusDto.EXPIRED_NOT_AUTHORIZED to
          Pair(
            TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            TransactionEventCode.TRANSACTION_EXPIRED_EVENT.toString()),
        TransactionStatusDto.CANCELLATION_EXPIRED to
          Pair(
            TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            TransactionEventCode.TRANSACTION_EXPIRED_EVENT.toString()),
        TransactionStatusDto.REFUNDED to
          Pair(
            TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()),
        TransactionStatusDto.CANCELED to
          Pair(
            TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            TransactionEventCode.TRANSACTION_CLOSED_EVENT.toString()),
        TransactionStatusDto.UNAUTHORIZED to
          Pair(
            TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            TransactionEventCode.TRANSACTION_CLOSED_EVENT.toString()))
  }

  /**
   * Adds span attributes for transactions in the NOTIFIED_OK state Note: NOTIFIED_KO is not a final
   * state
   */
  fun addSpanAttributesNotificationsFlowFromTransaction(
    tx: BaseTransactionWithUserReceipt,
    events: Flux<TransactionEvent<Any>>
  ) {
    if (tx.status != TransactionStatusDto.NOTIFIED_OK) {
      return
    }

    processTransactionEvents(tx, events)
  }

  /** Adds span attributes for transactions in some expired states */
  fun addSpanAttributesExpiredFlowFromTransaction(
    tx: BaseTransaction,
    events: Flux<TransactionEvent<Any>>
  ) {
    val isFinalStatus =
      tx.status == TransactionStatusDto.EXPIRED_NOT_AUTHORIZED ||
        tx.status == TransactionStatusDto.CANCELLATION_EXPIRED

    if (!isFinalStatus) {
      return
    }

    processTransactionEvents(tx, events)
  }

  /** Adds span attributes for transactions in the REFUNDED state */
  fun addSpanAttributesRefundedFlowFromTransaction(
    tx: BaseTransaction,
    events: Flux<BaseTransactionEvent<Any>>
  ) {
    if (tx.status != TransactionStatusDto.REFUNDED) {
      return
    }

    processTransactionEvents(tx, events)
  }

  /** Adds span attributes for transactions in canceled or unauthorized states */
  fun addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
    tx: BaseTransaction,
    events: Flux<TransactionEvent<Any>>
  ) {
    if (tx.status != TransactionStatusDto.CANCELED &&
      tx.status != TransactionStatusDto.UNAUTHORIZED) {
      return
    }

    processTransactionEvents(tx, events)
  }

  /** Common processing logic for transaction events */
  private fun processTransactionEvents(tx: BaseTransaction, events: Flux<*>) {
    events
      .collectMap(
        { event -> (event as BaseTransactionEvent<*>).eventCode },
        { event -> (event as BaseTransactionEvent<*>).creationDate })
      .map { eventDateMap -> buildAttributesFromEvents(tx, eventDateMap) }
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

  /** Builds OpenTelemetry attributes from transaction and event data */
  private fun buildAttributesFromEvents(
    tx: BaseTransaction,
    eventDateMap: Map<String, String>
  ): Attributes {
    // Calculate durations
    val authorizationDuration =
      calculateDurationMs(
        eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()],
        eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT.toString()])

    val closePaymentToAddUserReceiptRequestedDuration =
      calculateDurationMs(
        eventDateMap[TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()],
        eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()])

    // Get appropriate event codes for total duration based on transaction status
    val totalDurationEvents = TOTAL_DURATION_EVENTS[tx.status]
    val totalDuration =
      if (totalDurationEvents != null) {
        calculateDurationMs(
          eventDateMap[totalDurationEvents.first], eventDateMap[totalDurationEvents.second])
      } else null

    // Build attributes
    val builder =
      Attributes.builder()
        .put(AttributeKey.stringKey(TRANSACTIONID), tx.transactionId.value())
        .put(AttributeKey.stringKey(TRANSACTIONSTATUS), tx.status.value)
        .put(AttributeKey.stringKey(CLIENTID), tx.clientId.toString())

    // Add payment method details if available
    when (tx) {
      is BaseTransactionWithUserReceipt -> {
        builder
          .put(AttributeKey.stringKey(PSPID), tx.transactionAuthorizationRequestData.pspId)
          .put(
            AttributeKey.stringKey(PAYMENTMETHOD),
            tx.transactionAuthorizationRequestData.paymentMethodName)
      }
      is BaseTransactionWithRequestedAuthorization -> {
        builder
          .put(AttributeKey.stringKey(PSPID), tx.transactionAuthorizationRequestData.pspId)
          .put(
            AttributeKey.stringKey(PAYMENTMETHOD),
            tx.transactionAuthorizationRequestData.paymentMethodName)
      }
    }

    // Add duration metrics if available
    totalDuration?.let { builder.put(AttributeKey.longKey(TRANSACTIONTOTALTIME), it) }
    authorizationDuration?.let {
      builder.put(AttributeKey.longKey(TRANSACTIONAUTHORIZATIONTIME), it)
    }
    closePaymentToAddUserReceiptRequestedDuration?.let {
      builder.put(AttributeKey.longKey(TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME), it)
    }

    return builder.build()
  }

  /**
   * Calculates the duration in milliseconds between two date strings.
   *
   * @param startDateString The start date as a string in ISO-8601 format, can be null
   * @param endDateString The end date as a string in ISO-8601 format, can be null
   * @return Duration in milliseconds, or null if either date is invalid or null
   */
  private fun calculateDurationMs(startDateString: String?, endDateString: String?): Long? {
    val startDate = parseDate(startDateString)
    val endDate = parseDate(endDateString)

    return if (startDate != null && endDate != null) {
      Duration.between(startDate, endDate).toMillis()
    } else null
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
