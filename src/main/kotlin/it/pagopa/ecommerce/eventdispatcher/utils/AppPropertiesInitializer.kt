package it.pagopa.ecommerce.eventdispatcher.utils

import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

@Configuration
class AppPropertiesInitializer(env: Environment) {
  init {
    AppProperties.env = env
  }
}
