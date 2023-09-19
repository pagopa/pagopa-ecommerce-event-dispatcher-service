package it.pagopa.ecommerce.eventdispatcher.config;

import it.pagopa.ecommerce.commons.redis.templatewrappers.v1.PaymentRequestInfoRedisTemplateWrapper;
import it.pagopa.ecommerce.commons.redis.templatewrappers.v1.RedisTemplateWrapperBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.time.Duration;

@Configuration
public class RedisConfig {

    @Bean
    public PaymentRequestInfoRedisTemplateWrapper paymentRequestInfoWrapper(
            RedisConnectionFactory redisConnectionFactory
    ) {
        // PaymentRequestInfo entities will have the same TTL
        // value
        return RedisTemplateWrapperBuilder.buildPaymentRequestInfoRedisTemplateWrapper(
                redisConnectionFactory,
                Duration.ofMinutes(10)
        );
    }
}
