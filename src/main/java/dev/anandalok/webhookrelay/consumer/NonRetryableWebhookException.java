package dev.anandalok.webhookrelay.consumer;

/**
 * The event is well-formed Kafka-wise but the consumer can't do anything useful
 * with it -- an event type it doesn't know, a payload missing a required field.
 * Retrying changes nothing about that, so this goes straight to the DLQ instead
 * of burning the backoff schedule first. See KafkaConsumerConfig.
 */
public class NonRetryableWebhookException extends RuntimeException {

    public NonRetryableWebhookException(String message) {
        super(message);
    }
}
