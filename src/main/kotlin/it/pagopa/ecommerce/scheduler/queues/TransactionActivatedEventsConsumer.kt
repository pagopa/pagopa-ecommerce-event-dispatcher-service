package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.generated.transactions.server.model.TransactionStatusDto
import it.pagopa.transactions.documents.TransactionActivatedEvent
import it.pagopa.transactions.documents.TransactionExpiredData
import it.pagopa.transactions.documents.TransactionExpiredEvent
import it.pagopa.transactions.domain.EmptyTransaction
import it.pagopa.transactions.domain.Transaction
import it.pagopa.transactions.domain.pojos.BaseTransaction
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import java.util.Objects
import java.util.UUID

@Service
class TransactionActivatedEventsConsumer(
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Objects>,
    @Autowired private val transactionsExpiredEventStoreRepository: TransactionsEventStoreRepository<TransactionExpiredData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository
) {

    var logger: Logger = LoggerFactory.getLogger(TransactionActivatedEventsConsumer::class.java)

    @ServiceActivator(inputChannel = "transactionactivatedchannel")
    fun messageReceiver(@Payload payload: ByteArray, @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer) {
        checkpointer.success().block()
        logger.info("Message '{}' successfully checkpointed", String(payload))

        val activatedEvent =
            BinaryData.fromBytes(payload).toObject(TransactionActivatedEvent::class.java)
        val transactionId = activatedEvent.transactionId

        transactionsEventStoreRepository.findByTransactionId(transactionId.toString())
            .reduce(EmptyTransaction(), Transaction::applyEvent).cast(BaseTransaction::class.java)
            .doOnSuccess { transaction ->
                transactionsExpiredEventStoreRepository.save(
                    TransactionExpiredEvent(
                        transaction.transactionId.value.toString(),
                        transaction.rptId.value,
                        activatedEvent.paymentToken,
                        TransactionExpiredData(transaction.status)
                    )
                )
                    .thenReturn(
                        transactionsViewRepository.save(
                            it.pagopa.transactions.documents.Transaction(
                                transaction.transactionId.value.toString(),
                                activatedEvent.paymentToken,
                                transaction.rptId.value,
                                transaction.description.value,
                                transaction.amount.value,
                                transaction.email.value,
                                TransactionStatusDto.EXPIRED
                            )
                        ).subscribe()
                    )

            }
            .flatMap {
                when (it.status.value) {

                    TransactionStatusDto.AUTHORIZATION_FAILED.value, TransactionStatusDto.AUTHORIZED.value, TransactionStatusDto.AUTHORIZATION_REQUESTED.value ->
                        mono {
                            paymentGatewayClient.requestRefund(
                                UUID.fromString(transactionId),
                            )
                        }
                    else -> {
                        mono {}
                    }
                }
            }.doOnSuccess { t ->

                logger.info(
                    "Transaction expired {}", t.toString()
                )

            }.doOnError { exception ->
                logger.error("Got exception while trying to handle event: ${exception.message}")
            }.block()

    }
}
