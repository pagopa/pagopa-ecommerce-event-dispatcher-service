package it.pagopa.ecommerce.scheduler

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.beans.factory.annotation.Autowired
import com.azure.spring.integration.storage.queue.inbound.StorageQueueMessageSource
import com.azure.spring.messaging.storage.queue.core.StorageQueueTemplate

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class SchedulerApplicationTests {

	@Autowired 
	lateinit var storageQueueMessageSource: StorageQueueMessageSource 

	@Autowired 
	lateinit var storageQueueTemplate: StorageQueueTemplate

	
	@Test
	fun contextLoads() {
	}

}
