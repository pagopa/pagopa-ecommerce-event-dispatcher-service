package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.utils.ConfidentialDataManager
import it.pagopa.generated.pdv.v1.ApiClient
import it.pagopa.generated.pdv.v1.api.TokenApi
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SecretConfigurations {
  @Bean("personalDataVaultApi")
  fun personalDataVaultApiClient(
    @Value("\${confidentialDataManager.personalDataVault.apiKey}") personalDataVaultApiKey: String,
    @Value("\${confidentialDataManager.personalDataVault.apiBasePath}") apiBasePath: String
  ): TokenApi {
    val pdvApiClient = ApiClient()
    pdvApiClient.setApiKey(personalDataVaultApiKey)
    pdvApiClient.setBasePath(apiBasePath)
    return TokenApi(pdvApiClient)
  }

  @Bean("sharedPersonalDataVaultApi")
  fun sharedPersonalDataVaultApiClient(
    @Value("\${confidentialDataManager.sharedPersonalDataVault.apiKey}")
    personalDataVaultApiKey: String,
    @Value("\${confidentialDataManager.personalDataVault.apiBasePath}") apiBasePath: String
  ): TokenApi {
    val pdvApiClient = ApiClient()
    pdvApiClient.setApiKey(personalDataVaultApiKey)
    pdvApiClient.setBasePath(apiBasePath)
    return TokenApi(pdvApiClient)
  }

  @Bean("eCommerceConfidentialDataManager")
  fun eCommerceConfidentialDataManager(
    @Qualifier("personalDataVaultApi") personalDataVaultApi: TokenApi
  ): ConfidentialDataManager {
    return ConfidentialDataManager(personalDataVaultApi)
  }

  @Bean("sharedConfidentialDataManager")
  fun sharedConfidentialDataManager(
    @Qualifier("sharedPersonalDataVaultApi") personalDataVaultApi: TokenApi
  ): ConfidentialDataManager {
    return ConfidentialDataManager(personalDataVaultApi)
  }
}
