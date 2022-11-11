package it.pagopa.ecommerce.scheduler

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import it.pagopa.ecommerce.scheduler.events.RetryEvent
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class SchedulerApplicationTests {
	
	@Test
	fun contextLoads() {
	}
}
