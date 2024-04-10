package it.pagopa.ecommerce.eventdispatcher.config

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.client.NpgClient.PaymentMethod
import it.pagopa.ecommerce.commons.utils.NpgApiKeyConfiguration
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
    @Value("\${npg.authorization.cards.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.CARDS)

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgPaypalApiKeys")
  @Bean
  fun npgPaypalApiKeys(
    @Value("\${npg.authorization.paypal.keys}") apiKeys: String,
    @Value("\${npg.authorization.paypal.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.PAYPAL)

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgBancomatPayApiKeys")
  @Bean
  fun npgBancomatPayApiKeys(
    @Value("\${npg.authorization.bancomatpay.keys}") apiKeys: String,
    @Value("\${npg.authorization.bancomatpay.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.BANCOMATPAY)

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgMyBankApiKeys")
  @Bean
  fun npgMyBankApiKeys(
    @Value("\${npg.authorization.mybank.keys}") apiKeys: String,
    @Value("\${npg.authorization.mybank.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.MYBANK)

  @Bean
  fun npgApiKeyHandler(
    npgCardsApiKeys: NpgPspApiKeysConfig,
    npgPaypalApiKeys: NpgPspApiKeysConfig,
    npgBancomatPayApiKeys: NpgPspApiKeysConfig,
    npgMyBankApiKeys: NpgPspApiKeysConfig,
    @Value("\${npg.client.apiKey}") defaultApiKey: String
  ) =
    NpgApiKeyConfiguration.Builder()
      .setDefaultApiKey(defaultApiKey)
      .withMethodPspMapping(PaymentMethod.CARDS, npgCardsApiKeys)
      .withMethodPspMapping(PaymentMethod.PAYPAL, npgPaypalApiKeys)
      .withMethodPspMapping(PaymentMethod.MYBANK, npgMyBankApiKeys)
      .withMethodPspMapping(PaymentMethod.BANCOMATPAY, npgBancomatPayApiKeys)
      .build()

  private fun parsePspApiKeyConfiguration(
    apiKeys: String,
    pspToHandle: Set<String>,
    paymentMethod: PaymentMethod
  ) =
    NpgPspApiKeysConfig.parseApiKeyConfiguration(apiKeys, pspToHandle, paymentMethod, objectMapper)
      .fold({ throw it }, { it })
}
