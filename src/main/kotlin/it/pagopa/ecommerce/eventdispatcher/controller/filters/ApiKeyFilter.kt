package it.pagopa.ecommerce.eventdispatcher.controller.filters

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class ApiKeyFilter(
  @Value("\${security.apiKey.securedPaths}") private val securedPaths: Set<String>,
  @Value("\${security.apiKey.primary}") private val primaryKey: String,
  @Value("\${security.apiKey.secondary}") private val secondaryKey: String
) : WebFilter {

  private val logger = LoggerFactory.getLogger(javaClass)

  private enum class ApiKeyType {
    PRIMARY,
    SECONDARY,
    UNKNOWN
  }

  private val validKeys =
    mapOf(primaryKey to ApiKeyType.PRIMARY, secondaryKey to ApiKeyType.SECONDARY)

  /*
   * @formatter:off
   *
   * Warning kotlin:S6508 - "Unit" should be used instead of "Void"
   * Suppressed because Spring WebFilter interface use Void as return type.
   *
   * @formatter:on
   */
  @SuppressWarnings("kotlin:S6508")
  override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
    val requestPath = exchange.request.path.toString()
    if (securedPaths.any { requestPath.startsWith(it) }) {
      val requestApiKey = getRequestApiKey(exchange)
      val isAuthorized =
        if (requestApiKey != null) {
          validKeys.keys.contains(requestApiKey)
        } else {
          false
        }
      if (!isAuthorized) {
        logger.error(
          "Unauthorized request for path: [{}], missing or invalid input [\"x-api-key\"] header",
          requestPath)
        exchange.response.statusCode = HttpStatus.UNAUTHORIZED
        return exchange.response.setComplete()
      }
      logMatchedApiKeyType(requestApiKey, requestPath)
    }
    return chain.filter(exchange)
  }

  private fun logMatchedApiKeyType(requestApiKey: String?, requestPath: String?) {
    val matchedKeyType: ApiKeyType =
      if (requestApiKey != null) {
        validKeys[requestApiKey] ?: ApiKeyType.UNKNOWN
      } else {
        ApiKeyType.UNKNOWN
      }
    logger.debug("Matched key: [{}] for path: [{}]", matchedKeyType, requestPath)
  }

  private fun getRequestApiKey(exchange: ServerWebExchange): String? {
    return exchange.request.headers.getFirst("x-api-key")
  }
}
