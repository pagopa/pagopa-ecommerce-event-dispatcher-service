package it.pagopa.ecommerce.eventdispatcher.utils

import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

@Component
object AppProperties {

  lateinit var env: Environment

  fun get(key: String): String? = env.getProperty(key)
  fun getOrDefault(key: String, default: String): String = env.getProperty(key) ?: default
}
