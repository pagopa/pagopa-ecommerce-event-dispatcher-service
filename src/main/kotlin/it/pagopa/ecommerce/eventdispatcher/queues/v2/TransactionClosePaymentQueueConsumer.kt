package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentHelper
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionClosePaymentQueueConsumerV2")
class TransactionClosePaymentQueueConsumer(
  @Autowired private val closePaymentHelper: ClosePaymentHelper
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

  fun messageReceiver(
    parsedEvent:
      Either<
        QueueEvent<TransactionUserCanceledEvent>, QueueEvent<TransactionClosureRequestedEvent>>,
    checkPointer: Checkpointer
  ) = messageReceiver(parsedEvent, checkPointer, EmptyTransaction())

  fun messageReceiver(
    parsedEvent:
      Either<
        QueueEvent<TransactionUserCanceledEvent>, QueueEvent<TransactionClosureRequestedEvent>>,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val closePaymentEvent =
      parsedEvent.fold({ ClosePaymentEvent.canceled(it) }, { ClosePaymentEvent.requested(it) })

    return closePaymentHelper.closePayment(closePaymentEvent, checkPointer, emptyTransaction)
  }
}
