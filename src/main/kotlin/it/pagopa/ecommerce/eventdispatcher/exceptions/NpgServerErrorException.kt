package it.pagopa.ecommerce.eventdispatcher.exceptions

class NpgServerErrorException(detail: String, throwable: Throwable? = null) :
  RuntimeException("Bad gateway ${if (detail.isNotEmpty()) ": $detail" else ""}", throwable)
