package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.USER_ID
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.generated.ecommerce.userstats.api.UserStatsApi
import it.pagopa.generated.ecommerce.userstats.dto.GuestMethodLastUsageData
import it.pagopa.generated.ecommerce.userstats.dto.UserLastPaymentMethodRequest
import it.pagopa.generated.ecommerce.userstats.dto.WalletLastUsageData
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Stream
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class UserStatsServiceClientTest {

  private val userStatsApi: UserStatsApi = mock()

  private val userStatsServiceClient = UserStatsServiceClient(userStatsApi)

  @Test
  fun `Should save last method used successfully for wallet payment`() {
    // pre-requisites
    val userId = UUID.fromString(USER_ID)
    val walletLastUsageData =
      WalletLastUsageData().walletId(UUID.randomUUID()).date(OffsetDateTime.now())
    val userLastPaymentMethodRequest =
      UserLastPaymentMethodRequest().apply {
        this.userId = userId
        this.details = walletLastUsageData
      }
    given(userStatsApi.saveLastPaymentMethodUsed(userLastPaymentMethodRequest))
      .willReturn(Mono.empty())
    // test
    StepVerifier.create(userStatsServiceClient.saveLastUsage(userId, walletLastUsageData))
      .expectNext(Unit)
      .verifyComplete()
  }

  @Test
  fun `Should save last method used successfully for guest payment`() {
    // pre-requisites
    val userId = UUID.fromString(USER_ID)
    val guestMethodLastUsageData =
      GuestMethodLastUsageData().paymentMethodId(UUID.randomUUID()).date(OffsetDateTime.now())
    val userLastPaymentMethodRequest =
      UserLastPaymentMethodRequest().apply {
        this.userId = userId
        this.details = guestMethodLastUsageData
      }
    given(userStatsApi.saveLastPaymentMethodUsed(userLastPaymentMethodRequest))
      .willReturn(Mono.empty())
    // test
    StepVerifier.create(userStatsServiceClient.saveLastUsage(userId, guestMethodLastUsageData))
      .expectNext(Unit)
      .verifyComplete()
  }

  companion object {
    @JvmStatic
    fun `Service error method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          HttpStatus.BAD_REQUEST,
          RuntimeException(
            "Bad request exception for user stats service saveLastPaymentMethodUsed")),
        Arguments.of(
          HttpStatus.UNAUTHORIZED,
          RuntimeException(
            "Unauthorized exception for user stats service saveLastPaymentMethodUsed")),
        Arguments.of(
          HttpStatus.INTERNAL_SERVER_ERROR,
          BadGatewayException(
            "Bad Gateway exception for user stats service saveLastPaymentMethodUsed")))
  }

  @ParameterizedTest
  @MethodSource("Service error method source")
  fun `Should throw error for guest payment`(status: HttpStatus, expectedException: Throwable) {
    // pre-requisites
    val userId = UUID.fromString(USER_ID)
    val guestMethodLastUsageDataDto =
      GuestMethodLastUsageData().paymentMethodId(UUID.randomUUID()).date(OffsetDateTime.now())
    val userLastPaymentMethodRequest =
      UserLastPaymentMethodRequest().apply {
        this.userId = userId
        this.details = guestMethodLastUsageDataDto
      }
    given(userStatsApi.saveLastPaymentMethodUsed(userLastPaymentMethodRequest))
      .willReturn(
        Mono.error {
          WebClientResponseException.create(
            status.value(),
            expectedException.message!!,
            HttpHeaders.EMPTY,
            "ErrorMessage".encodeToByteArray(),
            StandardCharsets.UTF_8)
        })
    // test
    StepVerifier.create(userStatsServiceClient.saveLastUsage(userId, guestMethodLastUsageDataDto))
      .expectErrorMatches {
        Assertions.assertEquals(expectedException::class.java, it::class.java)
        Assertions.assertEquals(expectedException.message, it.message)
        true
      }
      .verify()
  }
}
