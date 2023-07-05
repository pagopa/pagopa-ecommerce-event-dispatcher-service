package it.pagopa.ecommerce.eventdispatcher.config

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
class Config {
  @Bean
  fun jacksonStrictTyping(): Jackson2ObjectMapperBuilderCustomizer =
    Jackson2ObjectMapperBuilderCustomizer { builder ->
      var typeResolver: StdTypeResolverBuilder =
        DefaultTypeResolverBuilder(
          ObjectMapper.DefaultTyping.EVERYTHING,
          BasicPolymorphicTypeValidator.builder()
            .allowIfBaseType("it.pagopa.ecommerce")
            .allowIfBaseType(List::class.java)
            .build())
      typeResolver = typeResolver.init(JsonTypeInfo.Id.CLASS, null)
      typeResolver = typeResolver.inclusion(JsonTypeInfo.As.PROPERTY)

      builder
        .defaultTyping(typeResolver)
        .featuresToEnable(
          DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL,
          DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES,
          DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .featuresToDisable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS)
    }
}
