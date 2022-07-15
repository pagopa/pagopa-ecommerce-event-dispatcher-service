package it.pagopa.ecommerce.scheduler.queues

import com.azure.spring.messaging.checkpoint.Checkpointer
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class TransactionAuthRequestedEventsConsumerTests {

    @Mock private lateinit var checkpointer: Checkpointer

    @InjectMocks
    private lateinit var transactionAuthRequestedEventsConsumer:
            TransactionAuthRequestedEventsConsumer

    @Test
    fun messageReceiverSuccessTest() {

        // preconditions
        Mockito.`when`(checkpointer.success()).thenReturn(Mono.empty())

        // test
        transactionAuthRequestedEventsConsumer.messageReceiver(
                "payload".toByteArray(),
                checkpointer
        )

        // Asserts
        Mockito.verify(checkpointer, Mockito.times(1)).success()
    }
}
