package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
object TransactionsViewProjectionHandler {

  lateinit var env: Environment

  // Flag to decide if we have to update the transactions-view collection (true) or not.
  const val ENV_TRANSACTIONSVIEW_UPDATE_ENABLED_FLAG = "transactionsview.update.enabled"

  @Autowired
  fun init(environment: Environment) {
    env = environment
  }

  fun saveEventIntoView(
    transaction: Transaction,
    transactionsViewRepository: TransactionsViewRepository,
    saveAction: (TransactionsViewRepository, Transaction) -> Mono<Transaction>,
  ): Mono<Transaction> {
    val saveEvent = env.getProperty(ENV_TRANSACTIONSVIEW_UPDATE_ENABLED_FLAG, "true").toBoolean()
    return if (saveEvent) {
      saveAction(transactionsViewRepository, transaction)
    } else {
      Mono.just(transaction)
    }
  }
}
