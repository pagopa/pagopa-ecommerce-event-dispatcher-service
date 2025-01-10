package it.pagopa.ecommerce.eventdispatcher


import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Lazy
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication
@EnableIntegration
class EventDispatcherApplication {

  @Bean
  fun initializeLazyBeans(applicationContext: ApplicationContext): () -> Unit {
    return {
      applicationContext.getBeansWithAnnotation(Lazy::class.java).forEach { (name, _) ->
        applicationContext.getBean(name)
      }
    }
  }
}

fun main(args: Array<String>) {
  val context = runApplication<EventDispatcherApplication>(*args)
  val initializeLazyBeans = context.getBean("initializeLazyBeans") as () -> Unit
  initializeLazyBeans()
}
