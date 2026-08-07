package dev.anandalok.webhookrelay.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.UUID;

/**
 * Processes events off the topic.
 *
 * Idempotent by construction: the first thing it does is claim the event id in
 * processed_event. Because that is the primary key, a concurrent or repeat
 * delivery loses the race and is skipped. Kafka gives at-least-once and the
 * outbox can republish, so "we will see this twice" is a certainty, not an edge case.
 */
@Component
public class WebhookEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventConsumer.class);

    // The event types this consumer knows how to fan out. Anything else is not a
    // transient failure -- retrying it four times changes nothing -- so it's
    // classified non-retryable and routed straight to the DLQ.
    private static final Set<String> SUPPORTED_EVENT_TYPES =
            Set.of("order.created", "order.shipped", "order.cancelled");

    private final ProcessedEventRepository processed;
    private final DownstreamRecordRepository downstream;
    private final ObjectMapper json;
    private final Counter handled;
    private final Counter skipped;

    public WebhookEventConsumer(ProcessedEventRepository processed,
                                DownstreamRecordRepository downstream,
                                ObjectMapper json,
                                MeterRegistry meters) {
        this.processed = processed;
        this.downstream = downstream;
        this.json = json;
        this.handled = Counter.builder("relay.consumer.handled").register(meters);
        this.skipped = Counter.builder("relay.consumer.skipped.duplicate").register(meters);
    }

    @KafkaListener(topics = "${relay.topics.events}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void onEvent(String message) throws Exception {
        JsonNode node = json.readTree(message);
        UUID eventId = UUID.fromString(node.get("eventId").asText());

        if (processed.existsById(eventId)) {
            skipped.increment();
            log.debug("Skipping already-processed event {}", eventId);
            return;
        }

        // Claim before doing work. If the work below throws, the transaction rolls
        // back and the claim disappears with it, so a retry can pick it up cleanly.
        processed.save(new ProcessedEvent(eventId));

        handle(node);
        handled.increment();
    }

    /**
     * Fan-out target: the {@code downstream_record} projection. A real deployment
     * would replace this with a call to whatever the service actually exists to
     * sync -- another API, a search index, a data warehouse -- but the exception
     * classification below is the part that matters and transfers regardless of
     * what's on the other end.
     *
     * Retryable vs. permanent, decided here and written down because the decision
     * doesn't reveal itself just from reading the happy path:
     * - Unknown or missing event type / payload: permanent. The fourth retry sees
     *   the same malformed message as the first. Straight to the DLQ.
     * - Anything from the repository (connection drop, deadlock, downstream down):
     *   retryable by default -- not caught here, left to propagate so
     *   DefaultErrorHandler's backoff applies.
     */
    private void handle(JsonNode event) {
        String eventType = event.path("eventType").asText(null);
        JsonNode payload = event.path("payload");

        if (eventType == null || !SUPPORTED_EVENT_TYPES.contains(eventType)) {
            throw new NonRetryableWebhookException("Unsupported event type: " + eventType);
        }
        if (payload.isMissingNode() || payload.isNull()) {
            throw new NonRetryableWebhookException("Missing payload for event " + event.path("eventId").asText());
        }

        UUID eventId = UUID.fromString(event.get("eventId").asText());
        String provider = event.get("provider").asText();

        log.info("Syncing event type={} id={} to downstream store", eventType, eventId);
        downstream.save(new DownstreamRecord(eventId, provider, eventType, payload.toString()));
    }
}
