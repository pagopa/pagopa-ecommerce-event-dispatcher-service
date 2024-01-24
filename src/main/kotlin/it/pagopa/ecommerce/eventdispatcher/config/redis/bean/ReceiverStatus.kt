package it.pagopa.ecommerce.eventdispatcher.config.redis.bean

import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterHandlerService

/** Data class that contain all information about a specific event receiver */
data class ReceiverStatus(
  val receiverName: String,
  val receiverStatus: InboundChannelAdapterHandlerService.Status,
  val queriedAt: String
)
