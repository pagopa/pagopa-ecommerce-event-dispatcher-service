package it.pagopa.ecommerce.scheduler.client

import io.netty.handler.codec.http.HttpResponseStatus.*
import it.pagopa.ecommerce.scheduler.exceptions.BadGatewayException
import it.pagopa.ecommerce.scheduler.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.scheduler.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.gateway.v1.api.PaymentTransactionsControllerApi
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.util.*

@Component
class PaymentGatewayClient {
    @Autowired
    @Qualifier("paymentTransactionGatewayWebClient")
    private lateinit var paymentTransactionsControllerApi: PaymentTransactionsControllerApi


    fun requestRefund(requestId: UUID): Mono<PostePayRefundResponseDto> {
        return paymentTransactionsControllerApi.refundRequest(requestId)
            .onErrorMap {
                when(it){
                    NOT_FOUND -> TransactionNotFound(requestId)
                    GATEWAY_TIMEOUT -> GatewayTimeoutException()
                    INTERNAL_SERVER_ERROR -> BadGatewayException("")
                    else -> it
                }
            }
    }
}