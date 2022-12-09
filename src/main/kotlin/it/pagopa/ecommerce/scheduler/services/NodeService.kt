package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.commons.documents.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.domain.TransactionEventCode
import it.pagopa.ecommerce.scheduler.client.NodeClient
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.scheduler.queues.TransactionActivatedEventsConsumer
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.*

@Service
class NodeService(
    @Autowired private val nodeClient: NodeClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionAuthorizationRequestData>
) {
    var logger: Logger = LoggerFactory.getLogger(TransactionActivatedEventsConsumer::class.java)
    suspend fun closePayment(transactionId: UUID, transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum): ClosePaymentResponseDto {
        val transactionEventCode = TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT

        val authEvent = transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            transactionEventCode
        ).awaitSingleOrNull()
            ?: throw TransactionEventNotFoundException(transactionId, transactionEventCode)

        logger.info("Invoking closePayment with  outcome {}", transactionOutcome)

        val closePaymentRequest = ClosePaymentRequestV2Dto().apply {
            paymentTokens = listOf(authEvent.paymentToken)
            outcome = transactionOutcome
            idPSP = authEvent.data.pspId
            paymentMethod = authEvent.data.paymentTypeCode
            idBrokerPSP = authEvent.data.brokerName
            idChannel = authEvent.data.pspChannelCode
            this.transactionId = transactionId.toString()
            totalAmount = (authEvent.data.amount + authEvent.data.fee).toBigDecimal()
            timestampOperation = OffsetDateTime.now()
        }
        return nodeClient.closePayment(closePaymentRequest).awaitSingle()
    }
}