package it.pagopa.ecommerce.eventdispatcher.config

import com.azure.core.serializer.json.jackson.JacksonJsonSerializer
import com.azure.core.serializer.json.jackson.JacksonJsonSerializerBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AzureSDKConfig {
  @Bean
  fun jacksonJSONSerializer(objectMapper: ObjectMapper): JacksonJsonSerializer =
    JacksonJsonSerializerBuilder().serializer(objectMapper).build()
}
