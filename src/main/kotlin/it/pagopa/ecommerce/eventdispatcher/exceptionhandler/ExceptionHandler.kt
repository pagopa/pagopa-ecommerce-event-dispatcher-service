package it.pagopa.ecommerce.eventdispatcher.exceptionhandler

import it.pagopa.ecommerce.eventdispatcher.exceptions.NoEventReceiverStatusFound
import it.pagopa.generated.eventdispatcher.server.model.ProblemJsonDto
import jakarta.xml.bind.ValidationException
import javax.validation.ConstraintViolationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.bind.support.WebExchangeBindException
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import org.springframework.web.server.ServerWebInputException

/** Rest controller exception handler class */
@RestControllerAdvice
class ExceptionHandler {

  val logger: Logger = LoggerFactory.getLogger(javaClass)

  val invalidRequestDefaultMessage = "Input request is invalid."

  @ExceptionHandler(NoEventReceiverStatusFound::class)
  fun handleNoEventReceiverDataFound(
    e: NoEventReceiverStatusFound
  ): ResponseEntity<ProblemJsonDto> {
    logger.error("Exception processing request", e)
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
      .body(
        ProblemJsonDto(
          title = "Not found",
          status = HttpStatus.NOT_FOUND.value(),
          detail = "No data found for receiver statuses"))
  }

  @ExceptionHandler(RuntimeException::class)
  fun handleGenericException(e: RuntimeException): ResponseEntity<ProblemJsonDto> {
    logger.error("Exception processing request", e)
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
      .body(
        ProblemJsonDto(
          title = "Internal Server Error",
          status = HttpStatus.INTERNAL_SERVER_ERROR.value(),
          detail = "An unexpected error occurred processing the request"))
  }

  /** Validation request exception handler */
  @ExceptionHandler(
    MethodArgumentNotValidException::class,
    MethodArgumentTypeMismatchException::class,
    ServerWebInputException::class,
    ValidationException::class,
    HttpMessageNotReadableException::class,
    WebExchangeBindException::class,
    ConstraintViolationException::class)
  fun handleRequestValidationException(exception: Exception): ResponseEntity<ProblemJsonDto> {
    logger.error(invalidRequestDefaultMessage, exception)
    return ResponseEntity.badRequest()
      .body(
        ProblemJsonDto(
          status = HttpStatus.BAD_REQUEST.value(),
          title = "Bad request",
          detail = invalidRequestDefaultMessage))
  }
}
