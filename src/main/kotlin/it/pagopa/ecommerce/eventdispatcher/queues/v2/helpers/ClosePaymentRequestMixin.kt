package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
@JsonIgnoreProperties(value = [], allowSetters = true)
abstract class ClosePaymentRequestMixin
