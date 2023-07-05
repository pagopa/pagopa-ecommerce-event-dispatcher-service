package it.pagopa.ecommerce.eventdispatcher.config

import com.azure.core.serializer.json.jackson.JacksonJsonSerializerBuilder
import com.azure.core.util.serializer.JsonSerializer
import com.azure.core.util.serializer.JsonSerializerProvider
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import java.util.*

class StrictJsonSerializerProvider : JsonSerializerProvider {
  override fun createInstance(): JsonSerializer {
    val objectMapper =
      ObjectMapper()
        .activateDefaultTyping(
          BasicPolymorphicTypeValidator.builder()
            .allowIfBaseType("it.pagopa.ecommerce")
            .allowIfBaseType(Optional::class.java)
            .allowIfBaseType(List::class.java)
            .build(),
          ObjectMapper.DefaultTyping.EVERYTHING,
          JsonTypeInfo.As.PROPERTY)
        .registerModule(Jdk8Module())
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
        .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, true)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)

    return JacksonJsonSerializerBuilder().serializer(objectMapper).build()
  }
}
