package it.pagopa.ecommerce.eventdispatcher.exceptions

class NpgMissingRequiredFieldException(field: String, request: String) :
  RuntimeException("Missing required field $field in $request response")
