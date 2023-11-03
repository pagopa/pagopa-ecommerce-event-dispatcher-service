package it.pagopa.ecommerce.eventdispatcher.utils

enum class DeadLetterErrorCategory {
    SEND_PAYMENT_RESULT_TIMEOUT,
    REFUND_RETRY_NO_ATTEMPT_LEFT
}