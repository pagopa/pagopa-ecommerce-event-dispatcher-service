package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentRequestV2Dto
import it.pagopa.transactions.documents.TransactionAuthorizationRequestedEvent
import it.pagopa.transactions.documents.TransactionAuthorizationStatusUpdateData
import it.pagopa.transactions.utils.TransactionEventCode
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import java.util.*

@Service
class TransactionAuthRequestedEventsConsumer(
    @Autowired private val nodeService: NodeService,
    @Autowired private val refundService: RefundService,
    @Autowired private val transactionsAuthorizationStatusUpdateEventRepository: TransactionsEventStoreRepository<TransactionAuthorizationStatusUpdateData>,
) {

    var logger: Logger = LoggerFactory.getLogger(TransactionAuthRequestedEventsConsumer::class.java)

    @ServiceActivator(inputChannel = "transactionauthrequestedchannel")
    fun messageReceiver(@Payload payload: ByteArray, @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer) {
        checkpointer.success().block()
        logger.info("Message '{}' successfully checkpointed", String(payload))

        val authorizationRequestedEvent = BinaryData.fromBytes(payload).toObject(TransactionAuthorizationRequestedEvent::class.java)
        val transactionId = authorizationRequestedEvent.transactionId

        val statusUpdateEvent = transactionsAuthorizationStatusUpdateEventRepository.findByTransactionIdAndEventCode(transactionId, TransactionEventCode.TRANSACTION_AUTHORIZATION_STATUS_UPDATED_EVENT).block()

        if (statusUpdateEvent == null) {
            try {
                logger.info("Refunding transaction ${authorizationRequestedEvent.transactionId}")
                refundService.requestRefund(authorizationRequestedEvent.data.authorizationRequestId).block()
            } catch(exception: Exception) {
                logger.error("Got exception while trying to request refund: ${exception.message}")
                logger.error("Adding retry message for request refund")
            }

            logger.info("Sending KO to Nodo for transaction ${authorizationRequestedEvent.transactionId}")

            runBlocking {
                try {
                    nodeService.closePayment(
                        UUID.fromString(transactionId),
                        ClosePaymentRequestV2Dto.OutcomeEnum.KO
                    )
                } catch (exception: Exception) {
                    logger.error("Got exception while trying to close payment: ${exception.message}")
                    logger.error("Adding retry message for close payment")
                }
            }
        }
    }
}
