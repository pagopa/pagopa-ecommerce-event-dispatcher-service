package it.pagopa.ecommerce.eventdispatcher.config

import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

private val GDI_CHECK_MAXIMUM_TIMEOUT = 5.minutes
private val DELAY_AFTER_FIRST_GDI_ATTEMPT = 1.minutes

@Configuration
@EnableScheduling
class Config(
  @Value("\${transactionAuthorizationOutcomeWaiting.eventOffsetSeconds}")
  authorizationOutcomeWaitingOffsetSeconds: Int,
  @Value("\${transactionAuthorizationOutcomeWaiting.maxAttempts}")
  authorizationOutcomeWaitingMaxAttempts: Int
) {
  companion object {
    private val logger = LoggerFactory.getLogger(Config::class.java)
  }

  init {
    /**
     *
     * Retry intervals are computed linearly (see
     * [it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RetryEventService])
     *
     * The waited time at attempt `i` is then `sum_k=0^i {base * k}` which is equal to
     *
     * `base * i * (i + 1) / 2`
     */
    val retryTimes =
      (0 until authorizationOutcomeWaitingMaxAttempts)
        .map { authorizationOutcomeWaitingOffsetSeconds * it * (it + 1) / 2 }
        .map { it.seconds + DELAY_AFTER_FIRST_GDI_ATTEMPT }
    val lastRetryTime = retryTimes.last()

    if (lastRetryTime < GDI_CHECK_MAXIMUM_TIMEOUT) {
      val authorizationOutcomeWaitingOffsetSecondsLowerBound =
        (GDI_CHECK_MAXIMUM_TIMEOUT - DELAY_AFTER_FIRST_GDI_ATTEMPT).inWholeSeconds * 2 /
          ((authorizationOutcomeWaitingMaxAttempts - 1) * authorizationOutcomeWaitingMaxAttempts)

      logger.warn(
        """
          Authorization outcome waiting configuration doesn't cover 5 minutes timeout for GDI checks.
          With current configuration retries will be made at the following times (after first attempt): {}.
          Constraint violated: last retry (assuming {} after first attempt) with computed value {} >= 5m (GDI check timeout).
          With current maximum attempts ({}) you need to set `transactionAuthorizationOutcomeWaiting.eventOffsetSeconds` to be at least {}
        """,
        retryTimes,
        DELAY_AFTER_FIRST_GDI_ATTEMPT,
        lastRetryTime,
        authorizationOutcomeWaitingMaxAttempts,
        authorizationOutcomeWaitingOffsetSecondsLowerBound)
    }
  }
}
