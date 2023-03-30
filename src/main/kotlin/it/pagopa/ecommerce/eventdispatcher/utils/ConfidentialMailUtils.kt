package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.domain.Confidential
import it.pagopa.ecommerce.commons.domain.v1.Email
import it.pagopa.ecommerce.commons.utils.ConfidentialDataManager
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class ConfidentialMailUtils(
  @Autowired private val emailConfidentialDataManager: ConfidentialDataManager
) {

  private val logger = LoggerFactory.getLogger(ConfidentialMailUtils::class.java)

  suspend fun toEmail(encrypted: Confidential<Email>): Email {
    return emailConfidentialDataManager
      .decrypt(encrypted) { Email(it) }
      .doOnError { logger.error("Exception decrypting confidential data", it) }
      .awaitSingle()
  }
}
