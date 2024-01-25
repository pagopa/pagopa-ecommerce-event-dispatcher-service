package it.pagopa.ecommerce.eventdispatcher.config.redis.bean

/** Data class that contain all information about a specific event receiver */
data class ReceiversStatus(
  val consumerInstanceId: String,
  val queriedAt: String,
  val receiverStatuses: List<ReceiverStatus>
)
