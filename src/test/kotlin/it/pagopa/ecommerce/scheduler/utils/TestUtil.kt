package it.pagopa.ecommerce.scheduler.utils

import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import java.util.*

class TestUtil {

    companion object {
        fun getMockedRefundRequest(paymentId: String?, result: String = "success"): PostePayRefundResponseDto {
             if (result == "success") {
                return PostePayRefundResponseDto()
                    .requestId(UUID.randomUUID().toString())
                    .refundOutcome(result)
                    .error("")
                    .paymentId(paymentId ?: UUID.randomUUID().toString())
            } else {
                 return PostePayRefundResponseDto()
                     .requestId(UUID.randomUUID().toString())
                     .refundOutcome(result)
                     .error(result)
                     .paymentId(paymentId ?: UUID.randomUUID().toString())
             }
        }
    }
}