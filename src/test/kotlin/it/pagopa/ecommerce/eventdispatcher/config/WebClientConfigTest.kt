package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.generated.ecommerce.nodo.v2.dto.CardAdditionalPaymentInformationsDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.CardClosePaymentRequestV2Dto
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.List
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.junit.jupiter.MockitoExtension

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
internal class WebClientConfigTest {
  @InjectMocks private lateinit var webClientsConfig: WebClientConfig

  @Test
  fun `should correctly serialize closePayment request`() = runTest {

    // Precondition
    val additionalPaymentInformationsDto =
      CardAdditionalPaymentInformationsDto()
        .outcomePaymentGateway(CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK)
        .totalAmount(BigDecimal(101).toString())
        .rrn("rrn")
        .fee(BigDecimal(1).toString())
        .timestampOperation(
          OffsetDateTime.now()
            .truncatedTo(ChronoUnit.SECONDS)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

    val closePaymentRequest =
      CardClosePaymentRequestV2Dto()
        .paymentTokens(List.of("paymentToken"))
        .outcome(CardClosePaymentRequestV2Dto.OutcomeEnum.OK)
        .idPSP("identificativoPsp")
        .idBrokerPSP("identificativoIntermediario")
        .idChannel("identificativoCanale")
        .transactionId("transactionId")
        .fee(BigDecimal(1))
        .timestampOperation(OffsetDateTime.now())
        .totalAmount(BigDecimal(101))
        .additionalPaymentInformations(additionalPaymentInformationsDto)

    val mapper = webClientsConfig.getNodeObjectMapper()
    val jsonRequest = mapper.writeValueAsString(closePaymentRequest)

    // Asserts
    Assertions.assertThat(jsonRequest).contains("\"rrn\":\"rrn\"")
  }

  @Test
  fun `should correctly serialize closePayment request with null value`() = runTest {

    // Precondition
    val additionalPaymentInformationsDto =
      CardAdditionalPaymentInformationsDto()
        .outcomePaymentGateway(CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK)
        .totalAmount(BigDecimal(101).toString())
        .rrn(null)
        .fee(BigDecimal(1).toString())
        .timestampOperation(
          OffsetDateTime.now()
            .truncatedTo(ChronoUnit.SECONDS)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

    val closePaymentRequest =
      CardClosePaymentRequestV2Dto()
        .paymentTokens(List.of("paymentToken"))
        .outcome(CardClosePaymentRequestV2Dto.OutcomeEnum.OK)
        .idPSP("identificativoPsp")
        .idBrokerPSP("identificativoIntermediario")
        .idChannel("identificativoCanale")
        .transactionId("transactionId")
        .fee(BigDecimal(1))
        .timestampOperation(OffsetDateTime.now())
        .totalAmount(BigDecimal(101))
        .additionalPaymentInformations(additionalPaymentInformationsDto)

    val mapper = webClientsConfig.getNodeObjectMapper()
    val jsonRequest = mapper.writeValueAsString(closePaymentRequest)

    // Asserts
    Assertions.assertThat(jsonRequest).doesNotContain("rrn")
  }
}
