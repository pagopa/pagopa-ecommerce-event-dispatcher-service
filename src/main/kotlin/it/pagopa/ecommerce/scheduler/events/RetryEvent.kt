package it.pagopa.ecommerce.scheduler.events

data class RetryEvent<T>(val retryCount: Int, val event: T)
