package it.pagopa.ecommerce.eventdispatcher.client

import java.time.Instant
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class UserStatsServiceClient {

  val logger: Logger = LoggerFactory.getLogger(UserStatsServiceClient::class.java)

  fun saveLastUsage(userId: String, methodId: UUID, date: Instant, isWallet: Boolean): Mono<Void> {
    logger.info(
      "User Id " +
        userId +
        " methodId " +
        methodId +
        "isWallet " +
        isWallet +
        " creation date " +
        date)
    return Mono.empty()
  }
}
