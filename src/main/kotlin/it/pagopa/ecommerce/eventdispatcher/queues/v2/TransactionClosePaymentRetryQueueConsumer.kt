package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentHelper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.*

@Service("TransactionClosePaymentRetryQueueConsumerV2")
class TransactionClosePaymentRetryQueueConsumer(
  @Autowired private val closePaymentHelper: ClosePaymentHelper
) {
  var logger: Logger =
    LoggerFactory.getLogger(TransactionClosePaymentRetryQueueConsumer::class.java)

  fun messageReceiver(
    parsedEvent:
      Either<QueueEvent<TransactionClosureErrorEvent>, QueueEvent<TransactionClosureRetriedEvent>>,
    checkPointer: Checkpointer
  ) = messageReceiver(parsedEvent, checkPointer, EmptyTransaction())

  fun messageReceiver(
    parsedEvent:
      Either<QueueEvent<TransactionClosureErrorEvent>, QueueEvent<TransactionClosureRetriedEvent>>,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val closePaymentEvent =
      parsedEvent.fold({ ClosePaymentEvent.errored(it) }, { ClosePaymentEvent.retried(it) })

    return closePaymentHelper.closePayment(closePaymentEvent, checkPointer, emptyTransaction)
  }
}
