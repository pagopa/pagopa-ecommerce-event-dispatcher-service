package it.pagopa.ecommerce.eventdispatcher.events

data class RetryEvent<T>(val retryCount: Int, val event: T)
