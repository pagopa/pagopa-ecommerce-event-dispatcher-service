package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.domain.v1.PaymentNotice
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.templates.success.RefNumberTemplate.Type
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class UserReceiptMailBuilder(@Autowired private val confidentialMailUtils: ConfidentialMailUtils) {

  suspend fun buildNotificationEmailRequestDto(
    baseTransactionWithRequestedUserReceipt: BaseTransactionWithRequestedUserReceipt
  ): NotificationEmailRequestDto {
    val sendPaymentResultOutcome =
      baseTransactionWithRequestedUserReceipt.transactionUserReceiptData.responseOutcome
    confidentialMailUtils.toEmail(baseTransactionWithRequestedUserReceipt.email).let { email ->
      return when (sendPaymentResultOutcome!!) {
        TransactionUserReceiptData.Outcome.OK ->
          buildOkMail(baseTransactionWithRequestedUserReceipt, email.value).let { okMail ->
            NotificationEmailRequestDto()
              .language(okMail.language)
              .subject(okMail.subject)
              .to(okMail.to)
              .templateId(NotificationsServiceClient.SuccessTemplateRequest.TEMPLATE_ID)
              .parameters(okMail.templateParameters)
          }
        TransactionUserReceiptData.Outcome.KO ->
          buildKoMail(baseTransactionWithRequestedUserReceipt, email.value).let { koMail ->
            NotificationEmailRequestDto()
              .language(koMail.language)
              .subject(koMail.subject)
              .to(koMail.to)
              .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
              .parameters(koMail.templateParameters)
          }
        else ->
          throw RuntimeException(
            "Unexpected transaction user receipt data response outcome ${baseTransactionWithRequestedUserReceipt.transactionUserReceiptData.responseOutcome} for transaction with id: ${baseTransactionWithRequestedUserReceipt.transactionId.value()}")
      }
    }
  }

  private fun buildKoMail(
    baseTransactionWithRequestedUserReceipt: BaseTransactionWithRequestedUserReceipt,
    emailAddress: String
  ): NotificationsServiceClient.KoTemplateRequest {
    val language = "it-IT"
    return NotificationsServiceClient.KoTemplateRequest(
      to = emailAddress,
      language = language,
      subject = "Il pagamento non è riuscito",
      templateParameters =
        KoTemplate(
          it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
            baseTransactionWithRequestedUserReceipt.transactionId
              .value()
              .toString()
              .lowercase(Locale.getDefault()),
            dateTimeToHumanReadableString(
              baseTransactionWithRequestedUserReceipt.creationDate,
              Locale.forLanguageTag(language)),
            amountToHumanReadableString(
              baseTransactionWithRequestedUserReceipt.paymentNotices
                .stream()
                .mapToInt { paymentNotice: PaymentNotice ->
                  paymentNotice.transactionAmount().value()
                }
                .sum()))))
  }

  private fun buildOkMail(
    baseTransactionWithRequestedUserReceipt: BaseTransactionWithRequestedUserReceipt,
    emailAddress: String
  ): NotificationsServiceClient.SuccessTemplateRequest {
    val language = "it-IT"
    val transactionAuthorizationRequestData =
      baseTransactionWithRequestedUserReceipt.transactionAuthorizationRequestData
    val transactionAuthorizationCompletedData =
      baseTransactionWithRequestedUserReceipt.transactionAuthorizationCompletedData
    val transactionUserReceiptData =
      baseTransactionWithRequestedUserReceipt.transactionUserReceiptData
    return NotificationsServiceClient.SuccessTemplateRequest(
      to = emailAddress,
      language = transactionUserReceiptData.language,
      subject = "Il riepilogo del tuo pagamento",
      templateParameters =
        SuccessTemplate(
          TransactionTemplate(
            baseTransactionWithRequestedUserReceipt.transactionId
              .value()
              .toString()
              .lowercase(Locale.getDefault()),
            dateTimeToHumanReadableString(
              ZonedDateTime.parse(transactionUserReceiptData.paymentDate),
              Locale.forLanguageTag(language)),
            amountToHumanReadableString(
              baseTransactionWithRequestedUserReceipt.paymentNotices
                .stream()
                .mapToInt { paymentNotice: PaymentNotice ->
                  paymentNotice.transactionAmount().value()
                }
                .sum() + transactionAuthorizationRequestData.fee),
            PspTemplate(
              transactionAuthorizationRequestData.pspBusinessName,
              FeeTemplate(amountToHumanReadableString(transactionAuthorizationRequestData.fee))),
            transactionAuthorizationCompletedData.rrn
              ?: transactionAuthorizationRequestData.authorizationRequestId,
            transactionAuthorizationCompletedData.authorizationCode,
            PaymentMethodTemplate(
              transactionAuthorizationRequestData.paymentMethodDescription,
              transactionAuthorizationRequestData.logo.toString(),
              null,
              false)),
          UserTemplate(null, emailAddress),
          CartTemplate(
            baseTransactionWithRequestedUserReceipt.paymentNotices
              .stream()
              .map { paymentNotice ->
                ItemTemplate(
                  RefNumberTemplate(Type.CODICE_AVVISO, paymentNotice.rptId().noticeId),
                  null,
                  PayeeTemplate(
                    transactionUserReceiptData.receivingOfficeName ?: "",
                    paymentNotice.rptId().fiscalCode),
                  transactionUserReceiptData.paymentDescription,
                  amountToHumanReadableString(paymentNotice.transactionAmount().value()))
              }
              .toList(),
            amountToHumanReadableString(
              baseTransactionWithRequestedUserReceipt.paymentNotices
                .stream()
                .mapToInt { paymentNotice -> paymentNotice.transactionAmount().value() }
                .sum()))))
  }

  fun amountToHumanReadableString(amount: Int): String {
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

  fun dateTimeToHumanReadableString(dateTime: ZonedDateTime, locale: Locale): String {
    val formatter = DateTimeFormatter.ofPattern("dd LLLL yyyy, kk:mm:ss").withLocale(locale)
    return dateTime.format(formatter)
  }
}
