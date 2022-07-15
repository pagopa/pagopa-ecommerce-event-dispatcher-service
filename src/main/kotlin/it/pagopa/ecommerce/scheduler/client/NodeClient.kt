package it.pagopa.ecommerce.scheduler.client

import it.pagopa.ecommerce.scheduler.exceptions.BadGatewayException
import it.pagopa.ecommerce.scheduler.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.scheduler.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.nodo.v1.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentResponseDto
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import java.util.*

@Component
class NodeClient(
    @Autowired private val nodeApi: NodoApi
) {
    suspend fun closePayment(closePaymentRequest: ClosePaymentRequestV2Dto): Mono<ClosePaymentResponseDto> {
        return mono {
            try {
                return@mono nodeApi.closePaymentV2(closePaymentRequest).awaitSingle()
            } catch (exception: WebClientResponseException) {
                throw when (exception.statusCode) {
                    HttpStatus.NOT_FOUND -> TransactionNotFound(UUID.fromString(closePaymentRequest.transactionId))
                    HttpStatus.REQUEST_TIMEOUT -> GatewayTimeoutException()
                    HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
                    else -> exception
                }
            }
        }
    }
}