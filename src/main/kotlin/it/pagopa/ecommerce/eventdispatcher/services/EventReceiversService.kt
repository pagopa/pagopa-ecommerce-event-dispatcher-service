package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherCommandsTemplateWrapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/** This class handles all InboundChannelsAdapters events receivers */
@Service
class EventReceiversService(
  @Autowired
  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper
) {

  fun shutdownAllReceiver(): Mono<Void> = Mono.empty()

  fun startAllReceiver(): Mono<Void> = Mono.empty()

  fun getReceiversStatus(): Mono<Void> = Mono.empty()
}
