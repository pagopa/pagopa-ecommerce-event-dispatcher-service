package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.UserReceiptMailBuilder
import it.pagopa.generated.notifications.templates.success.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.*

@Service
class TransactionNotificationsQueueConsumer(
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired
    private val transactionUserReceiptRepository:
    TransactionsEventStoreRepository<TransactionUserReceiptData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val notificationRetryService: NotificationRetryService,
    @Autowired private val userReceiptMailBuilder: UserReceiptMailBuilder,
    @Autowired private val notificationsServiceClient: NotificationsServiceClient,
    @Autowired
    private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val refundRetryService: RefundRetryService,
) {
    var logger: Logger = LoggerFactory.getLogger(TransactionNotificationsQueueConsumer::class.java)

    private fun getTransactionIdFromPayload(data: BinaryData) =
        data.toObjectAsync(TransactionUserReceiptRequestedEvent::class.java).map { it.transactionId }

    @ServiceActivator(inputChannel = "transactionnotificationschannel", outputChannel = "nullChannel")
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
    ) = messageReceiver(payload, checkPointer, EmptyTransaction())

    fun messageReceiver(
        payload: ByteArray,
        checkPointer: Checkpointer,
        emptyTransaction: EmptyTransaction
    ): Mono<Void> {
        val checkpoint = checkPointer.success()
        val binaryData = BinaryData.fromBytes(payload)
        val transactionId = getTransactionIdFromPayload(binaryData)
        val baseTransaction =
            reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
        val notificationResendPipeline =
            baseTransaction
                .flatMap {
                    logger.info("Status for transaction ${it.transactionId.value}: ${it.status}")

                    if (it.status != TransactionStatusDto.NOTIFICATION_REQUESTED) {
                        Mono.error(
                            BadTransactionStatusException(
                                transactionId = it.transactionId,
                                expected = TransactionStatusDto.NOTIFICATION_REQUESTED,
                                actual = it.status
                            )
                        )
                    } else {
                        Mono.just(it)
                    }
                }
                .cast(BaseTransactionWithRequestedUserReceipt::class.java)
                .flatMap { tx ->
                    mono { userReceiptMailBuilder.buildNotificationEmailRequestDto(tx) }
                        .flatMap { notificationsServiceClient.sendNotificationEmail(it) }
                        .flatMap {
                            updateNotifiedTransactionStatus(
                                tx, transactionsViewRepository, transactionUserReceiptRepository
                            )
                        }
                        .flatMap {

                            notificationRefundTransactionPipeline(
                                tx,
                                transactionsRefundedEventStoreRepository,
                                transactionsViewRepository,
                                paymentGatewayClient,
                                refundRetryService
                            )
                        }.then()
                        .onErrorResume { exception ->
                            logger.error(
                                "Got exception while retrying user receipt mail sending for transaction with id ${tx.transactionId}!",
                                exception
                            )
                            updateNotificationErrorTransactionStatus(
                                tx, transactionsViewRepository, transactionUserReceiptRepository
                            )
                                .flatMap {
                                    notificationRetryService.enqueueRetryEvent(tx, 0).doOnError { retryException ->
                                        logger.error("Exception enqueueing notification retry event", retryException)
                                    }
                                }
                                .then()
                        }
                }
        return checkpoint.then(notificationResendPipeline).then()
    }
}
