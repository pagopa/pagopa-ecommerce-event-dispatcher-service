package it.pagopa.ecommerce.eventdispatcher.config

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class NpgPspsApiKeyConfigBuilder {

  private val objectMapper = ObjectMapper()

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgCardsApiKeys")
  @Bean
  fun npgCardsApiKeys(
    @Value("\${npg.authorization.cards.keys}") apiKeys: String,
    @Value("\${npg.authorization.cards.pspList}") pspToHandle: Set<String?>
  ): NpgPspApiKeysConfig {
    return NpgPspApiKeysConfig.parseApiKeyConfiguration(
        apiKeys, pspToHandle, NpgClient.PaymentMethod.CARDS, objectMapper)
      .fold({ throw it }, { it })
  }
}
