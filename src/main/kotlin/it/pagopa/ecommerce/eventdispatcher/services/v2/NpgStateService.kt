package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GetStateException
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class NpgStateService(
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgCardsPspApiKey: NpgPspApiKeysConfig
) {

  fun getStateNpg(transactionId: UUID, sessionId: String, pspId: String): Mono<StateResponseDto> {
    return npgCardsPspApiKey[pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        npgClient.getState(UUID.randomUUID(), sessionId, apiKey).onErrorMap(
          NpgResponseException::class.java) { exception: NpgResponseException ->
          val responseStatusCode = exception.statusCode
          responseStatusCode
            .map {
              val errorCodeReason = "Received HTTP error code from NPG: $it"
              if (it.is5xxServerError) {
                BadGatewayException(errorCodeReason)
              } else {
                GetStateException(transactionId, errorCodeReason)
              }
            }
            .orElse(GetStateException(transactionId, "Unknown NPG HTTP response code"))
        }
      })
  }
}
