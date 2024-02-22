package it.pagopa.ecommerce.eventdispatcher.exceptions

class NpgServerErrorException(detail: String) :
  RuntimeException("Bad gateway ${if (detail.isNotEmpty()) ": $detail" else ""}")
