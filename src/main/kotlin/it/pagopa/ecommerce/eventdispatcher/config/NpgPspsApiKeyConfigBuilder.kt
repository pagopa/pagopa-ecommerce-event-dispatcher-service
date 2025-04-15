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

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgApplePayApiKeys")
  @Bean
  fun npgApplePayApiKeys(
    @Value("\${npg.authorization.applepay.keys}") apiKeys: String,
    @Value("\${npg.authorization.applepay.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.APPLEPAY)

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgSatispayApiKeys")
  @Bean
  fun npgSatispayApiKeys(
    @Value("\${npg.authorization.satispay.keys}") apiKeys: String,
    @Value("\${npg.authorization.satispay.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.SATISPAY)

  /**
   * Return a map where valued with each psp id - api keys entries
   *
   * @param apiKeys
   * - the secret api keys configuration json
   * @return the parsed map
   */
  @Qualifier("npgGooglePayApiKeys")
  @Bean
  fun npgGooglePayApiKeys(
    @Value("\${npg.authorization.googlepay.keys}") apiKeys: String,
    @Value("\${npg.authorization.googlepay.pspList}") pspToHandle: Set<String>
  ): NpgPspApiKeysConfig =
    parsePspApiKeyConfiguration(
      apiKeys = apiKeys, pspToHandle = pspToHandle, paymentMethod = PaymentMethod.GOOGLEPAY)

  @Bean
  fun npgApiKeyHandler(
    npgCardsApiKeys: NpgPspApiKeysConfig,
    npgPaypalApiKeys: NpgPspApiKeysConfig,
    npgBancomatPayApiKeys: NpgPspApiKeysConfig,
    npgMyBankApiKeys: NpgPspApiKeysConfig,
    npgApplePayApiKeys: NpgPspApiKeysConfig,
    npgSatispayApiKeys: NpgPspApiKeysConfig,
    npgGooglePayApiKeys: NpgPspApiKeysConfig,
    @Value("\${npg.client.apiKey}") defaultApiKey: String
  ) =
    NpgApiKeyConfiguration.Builder()
      .setDefaultApiKey(defaultApiKey)
      .withMethodPspMapping(PaymentMethod.CARDS, npgCardsApiKeys)
      .withMethodPspMapping(PaymentMethod.PAYPAL, npgPaypalApiKeys)
      .withMethodPspMapping(PaymentMethod.MYBANK, npgMyBankApiKeys)
      .withMethodPspMapping(PaymentMethod.BANCOMATPAY, npgBancomatPayApiKeys)
      .withMethodPspMapping(PaymentMethod.SATISPAY, npgSatispayApiKeys)
      .withMethodPspMapping(PaymentMethod.APPLEPAY, npgApplePayApiKeys)
      .withMethodPspMapping(PaymentMethod.GOOGLEPAY, npgGooglePayApiKeys)
      .build()

  private fun parsePspApiKeyConfiguration(
    apiKeys: String,
    pspToHandle: Set<String>,
    paymentMethod: PaymentMethod
  ) =
    NpgPspApiKeysConfig.parseApiKeyConfiguration(apiKeys, pspToHandle, paymentMethod, objectMapper)
      .fold({ throw it }, { it })
}
