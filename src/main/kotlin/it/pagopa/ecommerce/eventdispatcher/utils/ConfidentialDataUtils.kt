package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.domain.Confidential
import it.pagopa.ecommerce.commons.domain.Email
import it.pagopa.ecommerce.commons.utils.ConfidentialDataManager
import java.util.function.Function
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class ConfidentialDataUtils(
  @Autowired private val confidentialDataManager: ConfidentialDataManager
) {

  private val logger = LoggerFactory.getLogger(ConfidentialDataUtils::class.java)

  suspend fun toEmail(encrypted: Confidential<Email>): Email =
    decrypt(encrypted) { Email(it) }.awaitSingle()

  fun decryptGenericToken(opaqueToken: String): Mono<String> =
    decrypt(Confidential<StringConfidentialData>(opaqueToken)) { StringConfidentialData(it) }
      .map { it.clearValue }

  fun <T : ConfidentialDataManager.ConfidentialData> decrypt(
    encrypted: Confidential<T>,
    constructor: Function<String, T>
  ): Mono<T> =
    confidentialDataManager.decrypt(encrypted, constructor).doOnError {
      logger.error("Exception decrypting confidential data", it)
    }

  data class StringConfidentialData(val clearValue: String) :
    ConfidentialDataManager.ConfidentialData {
    override fun toStringRepresentation(): String = clearValue
  }
}
