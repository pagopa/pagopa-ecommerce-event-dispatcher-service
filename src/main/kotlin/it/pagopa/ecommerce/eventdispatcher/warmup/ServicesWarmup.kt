package it.pagopa.ecommerce.eventdispatcher.warmup

import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.hasAnnotation
import kotlin.system.measureTimeMillis
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.util.ClassUtils

@Component
class ServicesWarmup(
  @Autowired
  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService
) : ApplicationListener<ContextRefreshedEvent> {

  private val logger = LoggerFactory.getLogger(this.javaClass)

  override fun onApplicationEvent(event: ContextRefreshedEvent) {
    val eventReceiverServices =
      event.applicationContext
        .getBeansWithAnnotation(Service::class.java)
        .map { it.value }
        .filter { service ->
          service.javaClass.kotlin.declaredMemberFunctions.any {
            it.hasAnnotation<WarmupFunction>()
          }
        }
    LogTracingUtils.loggerTracingUtils()
      .success()
      .details(mapOf("services_count" to eventReceiverServices.size.toString()))
      .logInfo(logger, "Found services with warm-up functions")

    try {
      eventReceiverServices.forEach(this::warmUpService)
    } catch (e: Exception) {
      LogTracingUtils.loggerTracingUtils()
        .failure()
        .logErrorWithStackTrace(logger, e, "Exception during service warm-up")
    } finally {
      inboundChannelAdapterLifecycleHandlerService.invokeCommandForAllEndpoints("start")
    }
  }

  private fun warmUpService(serviceToWarmUpInstance: Any) {
    var warmUpMethods = 0
    val serviceToWarmUpKClass = ClassUtils.getUserClass(serviceToWarmUpInstance).kotlin
    val elapsedTime = measureTimeMillis {
      runCatching {
          serviceToWarmUpKClass.declaredMemberFunctions
            .filter { it.hasAnnotation<WarmupFunction>() }
            .forEach {
              warmUpMethods++
              val result: Result<*>
              val intertime = measureTimeMillis {
                result = runCatching { it.call(serviceToWarmUpInstance) }
              }
              LogTracingUtils.loggerTracingUtils()
                .success()
                .details(
                  mapOf(
                    "function" to it.toString(),
                    "elapsed_time_ms" to intertime.toString(),
                    "result" to result.toString()))
                .logInfo(logger, "Warmup function executed")
            }
        }
        .getOrElse {
          LogTracingUtils.loggerTracingUtils()
            .failure()
            .logErrorWithStackTrace(logger, it, "Exception performing service warm up")
        }
    }
    LogTracingUtils.loggerTracingUtils()
      .success()
      .details(
        mapOf(
          "service" to serviceToWarmUpKClass.toString(),
          "warmup_methods_count" to warmUpMethods.toString(),
          "elapsed_time_ms" to elapsedTime.toString()))
      .logInfo(logger, "Service warm-up completed")
  }
}
