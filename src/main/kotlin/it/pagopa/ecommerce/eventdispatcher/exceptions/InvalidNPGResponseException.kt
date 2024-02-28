package it.pagopa.ecommerce.eventdispatcher.exceptions

class InvalidNPGResponseException :
  RuntimeException("Npg response doesn't contains required fields")
