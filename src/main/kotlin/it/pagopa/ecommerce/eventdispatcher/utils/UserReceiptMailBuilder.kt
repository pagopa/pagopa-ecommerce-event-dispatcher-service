package it.pagopa.ecommerce.eventdispatcher.utils

import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.domain.v1.PaymentNotice
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithUserReceipt
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.templates.success.RefNumberTemplate.Type
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class UserReceiptMailBuilder(@Autowired private val confidentialMailUtils: ConfidentialMailUtils) {

  suspend fun buildNotificationEmailRequestDto(
    transactionWithUserReceiptError: BaseTransactionWithUserReceipt
  ): NotificationEmailRequestDto {
    val sendPaymentResultOutcome =
      transactionWithUserReceiptError.transactionUserReceiptData.responseOutcome
    confidentialMailUtils.toEmail(transactionWithUserReceiptError.email).apply {
      return when (sendPaymentResultOutcome!!) {
        TransactionUserReceiptData.Outcome.OK ->
          buildOkMail(transactionWithUserReceiptError, this.value)
        TransactionUserReceiptData.Outcome.KO ->
          buildKoMail(transactionWithUserReceiptError, this.value)
      }.fold(
        {
          NotificationEmailRequestDto()
            .language(it.language)
            .subject(it.subject)
            .to(it.to)
            .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
            .parameters(it.templateParameters)
        },
        {
          NotificationEmailRequestDto()
            .language(it.language)
            .subject(it.subject)
            .to(it.to)
            .templateId(NotificationsServiceClient.SuccessTemplateRequest.TEMPLATE_ID)
            .parameters(it.templateParameters)
        })
    }
  }

  private fun buildKoMail(
    baseTransactionWithUserReceipt: BaseTransactionWithUserReceipt,
    emailAddress: String
  ): Either<
    NotificationsServiceClient.KoTemplateRequest,
    NotificationsServiceClient.SuccessTemplateRequest> {
    val language = "it-IT"
    return Either.left(
      NotificationsServiceClient.KoTemplateRequest(
        to = emailAddress,
        language = language, // FIXME: Add language to AuthorizationRequestData
        subject = "Il pagamento non è riuscito",
        templateParameters =
          KoTemplate(
            it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
              baseTransactionWithUserReceipt.transactionId
                .value()
                .toString()
                .lowercase(Locale.getDefault()),
              dateTimeToHumanReadableString(
                OffsetDateTime.now(), // TODO add paymentDate to event
                Locale.forLanguageTag(language)),
              amountToHumanReadableString(
                baseTransactionWithUserReceipt.paymentNotices
                  .stream()
                  .mapToInt { paymentNotice: PaymentNotice ->
                    paymentNotice.transactionAmount().value()
                  }
                  .sum())))))
  }

  private fun buildOkMail(
    baseTransactionWithUserReceipt: BaseTransactionWithUserReceipt,
    emailAddress: String
  ): Either<
    NotificationsServiceClient.KoTemplateRequest,
    NotificationsServiceClient.SuccessTemplateRequest> {
    val language = "it-IT"
    val transactionAuthorizationRequestData =
      baseTransactionWithUserReceipt.transactionAuthorizationRequestData
    val transactionAuthorizationCompletedData =
      baseTransactionWithUserReceipt.transactionAuthorizationCompletedData
    val transactionUserReceiptData = baseTransactionWithUserReceipt.transactionUserReceiptData
    return Either.right(
      NotificationsServiceClient.SuccessTemplateRequest(
        to = emailAddress,
        language = transactionUserReceiptData.language,
        subject = "Il riepilogo del tuo pagamento",
        templateParameters =
          SuccessTemplate(
            // String id, String timestamp, String amount, PspTemplate psp, String rrn, String
            // authCode, PaymentMethodTemplate paymentMethod
            it.pagopa.generated.notifications.templates.success.TransactionTemplate(
              baseTransactionWithUserReceipt.transactionId
                .value()
                .toString()
                .lowercase(Locale.getDefault()),
              dateTimeToHumanReadableString(
                transactionUserReceiptData.paymentDate, Locale.forLanguageTag(language)),
              amountToHumanReadableString(
                baseTransactionWithUserReceipt.paymentNotices
                  .stream()
                  .mapToInt { paymentNotice: PaymentNotice ->
                    paymentNotice.transactionAmount().value()
                  }
                  .sum()),
              PspTemplate(
                transactionAuthorizationRequestData.pspBusinessName,
                FeeTemplate(amountToHumanReadableString(transactionAuthorizationRequestData.fee))),
              transactionAuthorizationRequestData.authorizationRequestId,
              transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(
                transactionAuthorizationRequestData.paymentMethodName,
                transactionUserReceiptData.paymentMethodLogo,
                null,
                false)),
            UserTemplate(null, emailAddress),
            CartTemplate(
              baseTransactionWithUserReceipt.paymentNotices
                .stream()
                .map { paymentNotice ->
                  ItemTemplate(
                    RefNumberTemplate(Type.CODICE_AVVISO, paymentNotice.rptId().noticeId),
                    null,
                    PayeeTemplate(
                      transactionUserReceiptData.paymentMethodLogo,
                      paymentNotice.rptId().fiscalCode),
                    transactionUserReceiptData.paymentDescription,
                    amountToHumanReadableString(paymentNotice.transactionAmount().value()))
                }
                .toList(),
              amountToHumanReadableString(
                baseTransactionWithUserReceipt.paymentNotices
                  .stream()
                  .mapToInt { paymentNotice -> paymentNotice.transactionAmount().value() }
                  .sum())))))
  }

  private fun amountToHumanReadableString(amount: Int): String {
    val repr = amount.toString()
    val centsSeparationIndex = 0.coerceAtLeast(repr.length - 2)
    var cents = repr.substring(centsSeparationIndex)
    var euros = repr.substring(0, centsSeparationIndex)
    if (euros.isEmpty()) {
      euros = "0"
    }
    if (cents.length == 1) {
      cents = "0$cents"
    }
    return "${euros},${cents}"
  }

  private fun dateTimeToHumanReadableString(dateTime: OffsetDateTime, locale: Locale): String {
    val formatter = DateTimeFormatter.ofPattern("dd LLLL yyyy, kk:mm:ss").withLocale(locale)
    return dateTime.format(formatter)
  }
}
