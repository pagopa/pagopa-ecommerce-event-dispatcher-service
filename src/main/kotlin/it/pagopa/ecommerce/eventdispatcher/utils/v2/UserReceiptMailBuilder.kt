package it.pagopa.ecommerce.eventdispatcher.utils.v2

import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.PaymentNotice
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.utils.ConfidentialDataUtils
import it.pagopa.ecommerce.eventdispatcher.utils.PaymentCode
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.templates.success.RefNumberTemplate.Type
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service("UserReceiptMailBuilderV2")
class UserReceiptMailBuilder(@Autowired private val confidentialDataUtils: ConfidentialDataUtils) {

  suspend fun buildNotificationEmailRequestDto(
    baseTransactionWithRequestedUserReceipt: BaseTransactionWithRequestedUserReceipt
  ): NotificationEmailRequestDto {
    val sendPaymentResultOutcome =
      baseTransactionWithRequestedUserReceipt.transactionUserReceiptData.responseOutcome
    confidentialDataUtils.toEmail(baseTransactionWithRequestedUserReceipt.email).let { email ->
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
    val transactionUserReceiptData =
      baseTransactionWithRequestedUserReceipt.transactionUserReceiptData
    val transactionAuthorizationRequestData =
      baseTransactionWithRequestedUserReceipt.transactionAuthorizationRequestData
    val transactionAuthorizationCompletedData =
      baseTransactionWithRequestedUserReceipt.transactionAuthorizationCompletedData
    return NotificationsServiceClient.SuccessTemplateRequest(
      to = emailAddress,
      language = transactionUserReceiptData.language,
      subject = "Il riepilogo del tuo pagamento",
      templateParameters =
        when (transactionAuthorizationRequestData.paymentTypeCode) {
          PaymentCode.PPAL.name,
          PaymentCode.MYBK.name ->
            createSuccessTemplate(
              baseTransactionWithRequestedUserReceipt = baseTransactionWithRequestedUserReceipt,
              emailAddress = emailAddress,
              authorizationCode = "-")
          PaymentCode.BPAY.name -> {
            val transactionGatewayAuthorizationData =
              transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
            if (transactionGatewayAuthorizationData is NpgTransactionGatewayAuthorizationData) {
              createSuccessTemplate(
                baseTransactionWithRequestedUserReceipt = baseTransactionWithRequestedUserReceipt,
                emailAddress = emailAddress,
                authorizationCode = transactionGatewayAuthorizationData.operationId)
            } else {
              throw RuntimeException(
                "Unexpected TransactionGatewayAuthorization for paymentTypeCode ${transactionAuthorizationRequestData.paymentTypeCode}. Expected NPG gateway.")
            }
          }
          PaymentCode.RBPB.name,
          PaymentCode.RBPP.name,
          PaymentCode.RBPR.name,
          PaymentCode.RBPS.name,
          PaymentCode.RPIC.name ->
            createSuccessTemplate(
              baseTransactionWithRequestedUserReceipt = baseTransactionWithRequestedUserReceipt,
              emailAddress = emailAddress,
              authorizationCode = transactionAuthorizationCompletedData.authorizationCode ?: "-")
          PaymentCode.CP.name -> {
            createSuccessTemplate(
              baseTransactionWithRequestedUserReceipt = baseTransactionWithRequestedUserReceipt,
              emailAddress = emailAddress,
              rrn = transactionAuthorizationCompletedData.rrn
                  ?: transactionAuthorizationRequestData.authorizationRequestId,
              authorizationCode = transactionAuthorizationCompletedData.authorizationCode ?: "-")
          }
          else ->
            throw IllegalArgumentException(
              "Unhandled or invalid payment type code: ${transactionAuthorizationRequestData.paymentTypeCode}")
        })
  }

  fun createSuccessTemplate(
    baseTransactionWithRequestedUserReceipt: BaseTransactionWithRequestedUserReceipt,
    emailAddress: String,
    rrn: String = "-",
    authorizationCode: String
  ): SuccessTemplate {
    val language = "it-IT"
    val transactionAuthorizationRequestData =
      baseTransactionWithRequestedUserReceipt.transactionAuthorizationRequestData

    return SuccessTemplate(
      TransactionTemplate(
        baseTransactionWithRequestedUserReceipt.transactionId
          .value()
          .toString()
          .lowercase(Locale.getDefault()),
        dateTimeToHumanReadableString(
          ZonedDateTime.parse(
            baseTransactionWithRequestedUserReceipt.transactionUserReceiptData.paymentDate),
          Locale.forLanguageTag(language)),
        amountToHumanReadableString(
          baseTransactionWithRequestedUserReceipt.paymentNotices
            .stream()
            .mapToInt { paymentNotice: PaymentNotice -> paymentNotice.transactionAmount().value() }
            .sum() + transactionAuthorizationRequestData.fee),
        PspTemplate(
          transactionAuthorizationRequestData.pspBusinessName,
          FeeTemplate(amountToHumanReadableString(transactionAuthorizationRequestData.fee))),
        rrn,
        authorizationCode,
        PaymentMethodTemplate(
          transactionAuthorizationRequestData.paymentMethodDescription,
          transactionAuthorizationRequestData.transactionGatewayAuthorizationRequestedData.logo
            .toString(),
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
                paymentNotice.companyName.value ?: "", paymentNotice.rptId().fiscalCode),
              paymentNotice.transactionDescription.value,
              amountToHumanReadableString(paymentNotice.transactionAmount().value()))
          }
          .toList(),
        amountToHumanReadableString(
          baseTransactionWithRequestedUserReceipt.paymentNotices
            .stream()
            .mapToInt { paymentNotice -> paymentNotice.transactionAmount().value() }
            .sum())))
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
