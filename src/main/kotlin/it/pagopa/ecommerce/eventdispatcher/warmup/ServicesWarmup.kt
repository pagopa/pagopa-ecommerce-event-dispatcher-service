package it.pagopa.ecommerce.eventdispatcher.warmup

import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.hasAnnotation
import kotlin.system.measureTimeMillis
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.getBeansWithAnnotation
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.util.ClassUtils

@Component
class ServicesWarmup : ApplicationListener<ContextRefreshedEvent> {

  private val logger = LoggerFactory.getLogger(this.javaClass)

  companion object {
    private var warmUpMethods = 0
    fun getWarmUpMethods(): Int {
      return warmUpMethods
    }
  }
  override fun onApplicationEvent(event: ContextRefreshedEvent) {
    val restservices =
      event.applicationContext
        .getBeansWithAnnotation(Service::class.java)
        .map { it.value }
        .filter { service ->
          service::class.java.methods.any { method -> method.name == "messageReceiver" }
        }
    logger.info("Found services: [{}]", restservices.size)
    restservices.forEach(this::warmUpservice)
  }

  private fun warmUpservice(serviceToWarmUpInstance: Any) {
    val serviceToWarmUpKClass = ClassUtils.getUserClass(serviceToWarmUpInstance).kotlin
    val elapsedTime = measureTimeMillis {
      runCatching {
          serviceToWarmUpKClass.declaredMemberFunctions
            .filter { it.hasAnnotation<WarmupFunction>() }
            .forEach {
              warmUpMethods++
              val result: Result<*>
              val intertime = measureTimeMillis {
                result = runCatching {
                  logger.info("Invoking function: [{}]", it.toString())
                  it.call(serviceToWarmUpInstance)
                }
              }
              logger.info(
                "Warmup function: [{}] -> elapsed time: [{}]. Is ok: [{}] ",
                it.toString(),
                intertime,
                result)
            }
        }
        .getOrElse { logger.error("Exception performing service warm up ", it) }
    }
    logger.info(
      "service: [{}] warm-up executed functions: [{}], elapsed time: [{}] ms",
      serviceToWarmUpKClass,
      warmUpMethods,
      elapsedTime)
  }
}
