package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.scheduler.client.NodeClient
import it.pagopa.ecommerce.scheduler.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v1.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentResponseDto
import it.pagopa.transactions.documents.TransactionAuthorizationRequestData
import it.pagopa.transactions.utils.TransactionEventCode
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.*

@Service
class NodeService(
    @Autowired private val nodeClient: NodeClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionAuthorizationRequestData>
) {
    suspend fun closePayment(transactionId: UUID, transactionOutcome: ClosePaymentRequestV2Dto.OutcomeEnum): ClosePaymentResponseDto {
        val transactionEventCode = TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT

        val authEvent = transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            transactionEventCode
        ).awaitSingleOrNull()
            ?: throw TransactionEventNotFoundException(transactionId, transactionEventCode)

        val closePaymentRequest = ClosePaymentRequestV2Dto().apply {
            paymentTokens = listOf(authEvent.paymentToken)
            outcome = transactionOutcome
            identificativoPsp = authEvent.data.pspId
            tipoVersamento = authEvent.data.paymentTypeCode
            identificativoIntermediario = authEvent.data.brokerName
            identificativoCanale = authEvent.data.pspChannelCode
            this.transactionId = transactionId.toString()
            totalAmount = (authEvent.data.amount + authEvent.data.fee).toBigDecimal()
            timestampOperation = OffsetDateTime.now()
        }

        return nodeClient.closePayment(closePaymentRequest).awaitSingle()
    }
}