package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.TransactionClosureErrorEvent
import it.pagopa.ecommerce.commons.documents.TransactionClosureSendData
import it.pagopa.ecommerce.commons.documents.TransactionClosureSentEvent
import it.pagopa.ecommerce.commons.domain.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.Transaction
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.pojos.BaseTransactionWithClosureError
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.scheduler.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
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
class TransactionClosureErrorEventConsumer(
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired private val transactionClosureSentEventRepository: TransactionsEventStoreRepository<TransactionClosureSendData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val nodeService: NodeService,
) {
    var logger: Logger = LoggerFactory.getLogger(TransactionClosureErrorEventConsumer::class.java)

    @ServiceActivator(inputChannel = "transactionclosureschannel", outputChannel = "nullChannel")
    fun messageReceiver(@Payload payload: ByteArray, @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer): Mono<TransactionClosureSentEvent> {
        checkpointer.success().block()

        // TODO: Add logic to try deserializing a retry event instead
        val closureErrorEvent =
            BinaryData.fromBytes(payload).toObject(TransactionClosureErrorEvent::class.java)
        val transactionId = closureErrorEvent.transactionId

        return transactionsEventStoreRepository.findByTransactionId(transactionId)
            .reduce(EmptyTransaction(), Transaction::applyEvent)
            .cast(BaseTransaction::class.java)
            .flatMap {
                if (it.status != TransactionStatusDto.CLOSURE_ERROR) {
                    Mono.error(
                        BadTransactionStatusException(
                            transactionId = TransactionId(UUID.fromString(transactionId)),
                            expected = TransactionStatusDto.CLOSURE_ERROR,
                            actual = it.status
                        )
                    )
                } else {
                    Mono.just(it)
                }
            }
            .cast(BaseTransactionWithClosureError::class.java)
            .flatMap { tx ->
                val closureOutcome = when (tx.transactionAuthorizationStatusUpdateData.authorizationResult) {
                    AuthorizationResultDto.OK -> ClosePaymentRequestV2Dto.OutcomeEnum.OK
                    AuthorizationResultDto.KO -> ClosePaymentRequestV2Dto.OutcomeEnum.KO
                    null -> return@flatMap Mono.error(RuntimeException("authorizationResult in status update event is null!"))
                }

                mono { nodeService.closePayment(tx.transactionId.value, closureOutcome) }
                    .flatMap { closePaymentResponse ->
                        updateTransactionStatus(tx, closePaymentResponse)
                    }
            }
            .onErrorMap { exception ->
                // TODO: Add appropriate retrying logic + enqueueing of retry event

                logger.error("Got exception while retrying closePayment!", exception)
                return@onErrorMap exception
            }
    }

    fun updateTransactionStatus(transaction: BaseTransactionWithClosureError, closePaymentResponseDto: ClosePaymentResponseDto): Mono<TransactionClosureSentEvent> {
        val newStatus = when (closePaymentResponseDto.outcome) {
            ClosePaymentResponseDto.OutcomeEnum.OK -> TransactionStatusDto.CLOSED
            ClosePaymentResponseDto.OutcomeEnum.KO -> TransactionStatusDto.CLOSURE_FAILED
        }

        val event = TransactionClosureSentEvent(
            transaction.transactionId.value.toString(),
            transaction.rptId.value,
            transaction.transactionActivatedData.paymentToken,
            TransactionClosureSendData(closePaymentResponseDto.outcome, newStatus)
        )

        logger.info("Updating transaction {} status to {}", transaction.transactionId.value, newStatus)

        val transactionUpdate = transactionsViewRepository.findByTransactionId(transaction.transactionId.value.toString())
        return transactionClosureSentEventRepository.save(event)
            .flatMap { closureSentEvent ->
                transactionUpdate.flatMap { tx ->
                    tx.status = newStatus
                    transactionsViewRepository.save(tx)
                }.thenReturn(closureSentEvent)
            }
    }
}
