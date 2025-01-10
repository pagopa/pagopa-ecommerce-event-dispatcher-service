package it.pagopa.ecommerce.payment.requests.warmup.utils

import com.azure.spring.messaging.checkpoint.Checkpointer
import reactor.core.publisher.Mono

object DummyCheckpointer : Checkpointer {
  override fun success(): Mono<Void> = Mono.empty()
  override fun failure(): Mono<Void> = Mono.empty()
}

object WarmupRequests {

  fun getTransactionAuthorizationRequestedEventV2(): ByteArray {
    val jsonString =
      """
        {
            "event": {
                "_class": "it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent",
                "id": "7ee814b9-8bb8-4f61-9204-2aa55cb56773",
                "transactionId": "00000000000000000000000000000000",
                "creationDate": "2025-01-10T14:28:47.843515440Z[Etc/UTC]",
                "data": {
                    "amount": 50000,
                    "fee": 0,
                    "paymentInstrumentId": "992ffbae-3ec3-4604-b8b4-c7c406d087b6",
                    "pspId": "CIPBITMM",
                    "paymentTypeCode": "CP",
                    "brokerName": "idBrokerPsp1",
                    "pspChannelCode": "idChannel1",
                    "paymentMethodName": "CARDS",
                    "pspBusinessName": "bundleName1",
                    "authorizationRequestId": "E1736519327527WJzV",
                    "paymentGateway": "NPG",
                    "paymentMethodDescription": "description2",
                    "transactionGatewayAuthorizationRequestedData": {
                        "type": "NPG",
                        "logo": "asset",
                        "brand": "VISA",
                        "sessionId": "sessionId",
                        "confirmPaymentSessionId": null,
                        "walletInfo": null
                    },
                    "pspOnUs": true
                },
                "eventCode": "TRANSACTION_AUTHORIZATION_REQUESTED_EVENT"
            },
            "tracingInfo": {
                "traceparent": "00-5868efa082297543570dafff7d53c70b-56f1d9262e6ee6cf-00",
                "tracestate": null,
                "baggage": null
            }
        }
        """
    return jsonString.toByteArray()
  }
}
