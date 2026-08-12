package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.generated.ecommerce.userstats.api.UserStatsApi
import it.pagopa.generated.ecommerce.userstats.dto.UserLastPaymentMethodData
import it.pagopa.generated.ecommerce.userstats.dto.UserLastPaymentMethodRequest
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class UserStatsServiceClient(
  @Autowired @Qualifier("userStatsServiceWebClient") private val userStatsServiceApi: UserStatsApi
) {

  val logger: Logger = LoggerFactory.getLogger(UserStatsServiceClient::class.java)

  fun saveLastUsage(
    userId: UUID,
    userLastPaymentMethodDataDto: UserLastPaymentMethodData
  ): Mono<Unit> {

    return userStatsServiceApi
      .saveLastPaymentMethodUsed(
        UserLastPaymentMethodRequest().userId(userId).details(userLastPaymentMethodDataDto))
      .doOnSuccess {
        LogTracingUtils.loggerTracingUtils()
          .success()
          .details(
            mapOf(
              "user_id" to userId.toString(),
              "last_used_method" to userLastPaymentMethodDataDto.toString()))
          .dependency("user-stats-service")
          .logInfo(logger, "Saved last method used for user")
      }
      .onErrorMap(WebClientResponseException::class.java) { exception: WebClientResponseException ->
        LogTracingUtils.loggerTracingUtils()
          .failure()
          .details(
            mapOf(
              "user_id" to userId.toString(),
              "last_used_method" to userLastPaymentMethodDataDto.toString(),
              "http_status" to exception.statusCode.toString(),
              "response_body" to exception.responseBodyAsString))
          .dependency("user-stats-service")
          .logError(logger, exception, "Failed to save last method used for user")
        when (exception.statusCode) {
          HttpStatus.BAD_REQUEST ->
            RuntimeException(
              "Bad request exception for user stats service saveLastPaymentMethodUsed")
          HttpStatus.UNAUTHORIZED ->
            RuntimeException(
              "Unauthorized exception for user stats service saveLastPaymentMethodUsed")
          HttpStatus.INTERNAL_SERVER_ERROR ->
            BadGatewayException(
              "Bad Gateway exception for user stats service saveLastPaymentMethodUsed")
          else -> exception
        }
      }
      .then(mono {})
  }
}
