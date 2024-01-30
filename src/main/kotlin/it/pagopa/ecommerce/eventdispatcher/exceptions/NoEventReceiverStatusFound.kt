package it.pagopa.ecommerce.eventdispatcher.exceptions

/** Exception thrown when no data can be found querying for event receiver statuses */
class NoEventReceiverStatusFound : RuntimeException("No event receiver status data found")
