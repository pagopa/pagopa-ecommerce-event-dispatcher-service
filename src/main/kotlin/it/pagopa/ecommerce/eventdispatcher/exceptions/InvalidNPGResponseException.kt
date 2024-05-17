package it.pagopa.ecommerce.eventdispatcher.exceptions

class InvalidNPGResponseException(
  override val message: String? = "Npg response doesn't contain required fields"
) : RuntimeException(message)
