package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgBadRequestException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgServerErrorException
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class NpgStateService(
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgCardsPspApiKey: NpgPspApiKeysConfig,
) {

  fun getStateNpg(
    transactionId: UUID,
    sessionId: String,
    pspId: String,
    correlationId: String
  ): Mono<StateResponseDto> {
    return npgCardsPspApiKey[pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        npgClient.getState(UUID.fromString(correlationId), sessionId, apiKey).onErrorMap(
          NpgResponseException::class.java) { exception: NpgResponseException ->
          val responseStatusCode = exception.statusCode
          responseStatusCode
            .map {
              val errorCodeReason = "Received HTTP error code from NPG: $it"
              if (it.is5xxServerError) {
                NpgServerErrorException(errorCodeReason)
              } else {
                NpgBadRequestException(transactionId, errorCodeReason)
              }
            }
            .orElse(NpgBadRequestException(transactionId, "Unknown NPG HTTP response code"))
        }
      })
  }
}
