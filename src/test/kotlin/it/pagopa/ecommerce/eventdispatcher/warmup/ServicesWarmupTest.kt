package it.pagopa.ecommerce.eventdispatcher.warmup

import it.pagopa.ecommerce.eventdispatcher.validation.BeanValidationConfiguration
import it.pagopa.generated.eventdispatcher.server.model.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource

@Import(BeanValidationConfiguration::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class ServicesWarmupTest {

  @Test fun `Should handle command creation successfully`() = runTest { assert(true) }
}
