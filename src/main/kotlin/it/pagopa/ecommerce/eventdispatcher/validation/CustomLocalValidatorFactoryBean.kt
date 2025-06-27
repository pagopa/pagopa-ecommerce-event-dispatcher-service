package it.pagopa.ecommerce.eventdispatcher.validation

import jakarta.validation.ClockProvider
import jakarta.validation.Configuration
import jakarta.validation.ParameterNameProvider
import java.lang.reflect.Constructor
import java.lang.reflect.Method
import org.hibernate.validator.internal.engine.DefaultClockProvider
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean

/**
 * This class customizes parameter name discovery for Kotlin suspend functions, working around
 * issues in default Hibernate Validator.
 *
 * See:
 * - Spring issue: https://github.com/spring-projects/spring-framework/issues/23499
 * - Hibernate issue: https://hibernate.atlassian.net/browse/HV-1638
 */
class CustomLocalValidatorFactoryBean : LocalValidatorFactoryBean() {

  override fun getClockProvider(): ClockProvider = DefaultClockProvider.INSTANCE

  override fun postProcessConfiguration(configuration: Configuration<*>) {
    super.postProcessConfiguration(configuration)
    val defaultProvider = configuration.defaultParameterNameProvider
    configuration.parameterNameProvider(
      object : ParameterNameProvider {
        override fun getParameterNames(constructor: Constructor<*>): List<String> {
          // Uses standard Java 8+ reflection with -parameters support
          val params = constructor.parameters
          return if (params.all { it.isNamePresent }) {
            params.map { it.name }
          } else {
            defaultProvider.getParameterNames(constructor)
          }
        }

        override fun getParameterNames(method: Method): List<String> {
          val params = method.parameters
          return if (params.all { it.isNamePresent }) {
            params.map { it.name }
          } else {
            defaultProvider.getParameterNames(method)
          }
        }
      })
  }
}
