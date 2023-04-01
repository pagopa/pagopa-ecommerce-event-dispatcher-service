package it.pagopa.ecommerce.eventdispatcher.utils

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.documents.v1.TransactionClosureData
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.domain.v1.Email
import it.pagopa.ecommerce.commons.domain.v1.RptId
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.mock

@OptIn(ExperimentalCoroutinesApi::class)
class UserReceiptMailBuilderTest {

  private val confidentialMailUtils: ConfidentialMailUtils = mock()

  private val userReceiptMailBuilder = UserReceiptMailBuilder(confidentialMailUtils)

  @Test
  fun `Should build success email for notified transaction with send payment result outcome OK`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialMailUtils.toEmail(any()))
        .willReturn(Email(TransactionTestUtils.EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
            as TransactionEvent<*>,
          TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)
            as TransactionEvent<*>,
          TransactionTestUtils.transactionUserReceiptRequestedEvent(
            TransactionTestUtils.transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        TransactionTestUtils.reduceEvents(*events.toTypedArray())
          as BaseTransactionWithRequestedUserReceipt
      val successTemplateRequest =
        NotificationsServiceClient.SuccessTemplateRequest(
          "foo@example.com",
          "Il riepilogo del tuo pagamento",
          "it-IT",
          SuccessTemplate(
            TransactionTemplate(
              baseTransaction.transactionId.value.toString(),
              baseTransaction.creationDate.toString(),
              "1,00",
              PspTemplate("pspBusinessName", FeeTemplate("0,10")),
              baseTransaction.transactionAuthorizationRequestData.authorizationRequestId,
              baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(
                TransactionTestUtils.PAYMENT_METHOD_NAME,
                TransactionTestUtils.PAYMENT_METHOD_LOGO_URL.toString(),
                null,
                false)),
            UserTemplate(DataTemplate(null, null, null), "foo@example.com"),
            CartTemplate(
              listOf(
                ItemTemplate(
                  RefNumberTemplate(
                    RefNumberTemplate.Type.CODICE_AVVISO,
                    RptId(TransactionTestUtils.RPT_ID).noticeId),
                  null,
                  PayeeTemplate(null, RptId(TransactionTestUtils.RPT_ID).fiscalCode),
                  TransactionTestUtils.PAYMENT_DESCRIPTION,
                  "1,00")),
              "1,00")))
      val expected =
        NotificationEmailRequestDto()
          .language(successTemplateRequest.language)
          .subject(successTemplateRequest.subject)
          .to(successTemplateRequest.to)
          .templateId(NotificationsServiceClient.SuccessTemplateRequest.TEMPLATE_ID)
          .parameters(successTemplateRequest.templateParameters)
      /*
       * Test
       */
      val notificationEmailRequest =
        userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction)
      /*
       * Assertions
       */
      val objectMapper = ObjectMapper()
      assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(notificationEmailRequest))
    }
}
