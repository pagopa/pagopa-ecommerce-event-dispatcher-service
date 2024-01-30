package it.pagopa.ecommerce.eventdispatcher

import it.pagopa.ecommerce.eventdispatcher.exceptionhandler.ExceptionHandler
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoEventReceiverStatusFound
import it.pagopa.generated.eventdispatcher.server.model.ProblemJsonDto
import jakarta.xml.bind.ValidationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus

class ExceptionHandlerTest {
  private val exceptionHandler = ExceptionHandler()

  @Test
  fun `Should handle NoResultFoundException`() {
    val exception = NoEventReceiverStatusFound()
    val response = exceptionHandler.handleNoEventReceiverDataFound(exception)
    assertEquals(
      ProblemJsonDto(
        status = HttpStatus.NOT_FOUND.value(),
        title = "Not found",
        detail = "No data found for receiver statuses"),
      response.body)
    assertEquals(HttpStatus.NOT_FOUND, response.statusCode)
  }

  @Test
  fun `Should handle ValidationExceptions`() {
    val exception = ValidationException("Invalid request")
    val response = exceptionHandler.handleRequestValidationException(exception)
    assertEquals(
      ProblemJsonDto(
        status = HttpStatus.BAD_REQUEST.value(),
        title = "Bad request",
        detail = "Input request is invalid."),
      response.body)
    assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
  }

  @Test
  fun `Should handle generic exception`() {
    val exception = NullPointerException("Nullpointer exception")
    val response = exceptionHandler.handleGenericException(exception)
    assertEquals(
      ProblemJsonDto(
        status = HttpStatus.INTERNAL_SERVER_ERROR.value(),
        title = "Internal Server Error",
        detail = "An unexpected error occurred processing the request"),
      response.body)
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
  }
}
