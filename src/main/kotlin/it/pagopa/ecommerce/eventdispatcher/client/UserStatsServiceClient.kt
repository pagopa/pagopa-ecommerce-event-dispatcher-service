package it.pagopa.ecommerce.eventdispatcher.client

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
    logger.info(
      "Saving last method used for user with id: [{}], last used method: [{}]",
      userId,
      userLastPaymentMethodDataDto)
    return userStatsServiceApi
      .saveLastPaymentMethodUsed(
        UserLastPaymentMethodRequest().userId(userId).details(userLastPaymentMethodDataDto))
      .onErrorMap(WebClientResponseException::class.java) { exception: WebClientResponseException ->
        logger.error("Error [${exception.statusCode}] for saveLastPaymentMethodUsed")
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
