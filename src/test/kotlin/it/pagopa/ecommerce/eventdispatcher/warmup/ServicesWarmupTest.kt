package it.pagopa.ecommerce.eventdispatcher.warmup

import it.pagopa.ecommerce.eventdispatcher.validation.BeanValidationConfiguration
import it.pagopa.generated.eventdispatcher.server.model.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Import
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.test.context.TestPropertySource

@Import(BeanValidationConfiguration::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class ServicesWarmupTest {

  private lateinit var servicesWarmup: ServicesWarmup
  private lateinit var applicationContext: ApplicationContext

  @BeforeEach
  fun setUp() {
    servicesWarmup = ServicesWarmup()
    applicationContext = mock(ApplicationContext::class.java)
  }

  @Test
  fun `Should execute all warmup functions`() = runTest {
    val event = ContextRefreshedEvent(applicationContext)
    servicesWarmup.onApplicationEvent(event)
    assertEquals(9, ServicesWarmup.getWarmUpMethods())
  }
}
