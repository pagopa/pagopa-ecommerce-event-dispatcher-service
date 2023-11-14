package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class NpgPspsApiKeyConfig {

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
  ): Map<String, String> {
    return NpgPspApiKeysConfig(apiKeys, pspToHandle, NpgClient.PaymentMethod.CARDS)
      .parseApiKeyConfiguration()
      .fold({ throw it }, { it })
  }
}
