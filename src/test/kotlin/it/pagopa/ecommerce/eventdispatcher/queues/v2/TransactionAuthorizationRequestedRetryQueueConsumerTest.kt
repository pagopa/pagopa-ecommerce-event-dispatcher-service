package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedRetriedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.WorkflowStateDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgBadRequestException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgServerErrorException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.transactionauthrequests.v1.dto.OutcomeNpgGatewayDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionStatusDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.time.OffsetDateTime
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TransactionAuthorizationRequestedRetryQueueConsumerTest {

  private val transactionsServiceClient: TransactionsServiceClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionAuthorizationRequestedRetriedEvent> = mock()

  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService =
    mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()

  private val checkpointer: Checkpointer = mock()

  private val transactionAuthorizationRequestedRetryQueueConsumer =
    TransactionAuthorizationRequestedRetryQueueConsumer(
      transactionsServiceClient = transactionsServiceClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
      authorizationStateRetrieverService = authorizationStateRetrieverService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2)

  companion object {
    @JvmStatic
    fun `Recover transaction status timestamp method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of("2024-01-01T00:00:00", OffsetDateTime.parse("2024-01-01T00:00:00+01:00")),
        Arguments.of("2024-08-01T00:00:00", OffsetDateTime.parse("2024-08-01T00:00:00+02:00")))
  }
}
