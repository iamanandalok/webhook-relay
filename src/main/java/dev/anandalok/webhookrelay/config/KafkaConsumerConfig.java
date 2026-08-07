package dev.anandalok.webhookrelay.config;

import dev.anandalok.webhookrelay.consumer.NonRetryableWebhookException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.apache.kafka.common.TopicPartition;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Retry with exponential backoff, then dead-letter.
     *
     * The backoff matters: a downstream outage retried immediately four times is
     * just four failures in 20ms, which is not a retry policy, it's a burst of noise.
     * Spacing them out gives the downstream a chance to actually recover.
     *
     * Anything that lands in the DLQ needs a human. Alert on depth > 0 rather than
     * hoping someone checks it.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                            RelayProperties props) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (ConsumerRecord<?, ?> record, Exception ex) ->
                        new TopicPartition(props.topics().dlq(), record.partition()));

        ExponentialBackOff backOff = new ExponentialBackOff(
                props.retry().initialBackoffMs(), props.retry().multiplier());
        backOff.setMaxElapsedTime(60_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Deserialization failures and unsupported/malformed events will never
        // succeed on retry, no matter how many times or how far apart. Straight
        // to the DLQ instead of burning the backoff schedule first.
        handler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class,
                IllegalArgumentException.class,
                NonRetryableWebhookException.class);

        return handler;
    }
}
