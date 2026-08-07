package dev.anandalok.webhookrelay;

import dev.anandalok.webhookrelay.api.WebhookRequest;
import dev.anandalok.webhookrelay.consumer.DownstreamRecordRepository;
import dev.anandalok.webhookrelay.consumer.ProcessedEventRepository;
import dev.anandalok.webhookrelay.ingest.WebhookIngestService;
import dev.anandalok.webhookrelay.outbox.OutboxMessage;
import dev.anandalok.webhookrelay.outbox.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WebhookIngestIntegrationTest extends AbstractIntegrationTest {

    @Autowired WebhookIngestService ingest;
    @Autowired OutboxRepository outbox;
    @Autowired ProcessedEventRepository processed;
    @Autowired DownstreamRecordRepository downstream;

    @Test
    void storesEventAndOutboxRowInOneTransaction() {
        var request = new WebhookRequest("evt-1001", "order.created", Map.of("orderId", "A-1"));

        var result = ingest.ingest("acme", request);

        assertThat(result.duplicate()).isFalse();
        assertThat(outbox.findAll())
                .anyMatch(m -> m.getAggregateId().equals(result.eventId())
                            && m.getMessageKey().equals("evt-1001"));
    }

    @Test
    void redeliveryOfSameExternalIdDoesNotCreateSecondEvent() {
        var request = new WebhookRequest("evt-2002", "order.created", Map.of("orderId", "B-2"));

        var first = ingest.ingest("acme", request);
        var second = ingest.ingest("acme", request);

        assertThat(second.duplicate()).isTrue();
        assertThat(second.eventId()).isEqualTo(first.eventId());

        long outboxRows = outbox.findAll().stream()
                .filter(m -> m.getAggregateId().equals(first.eventId()))
                .count();
        assertThat(outboxRows).isEqualTo(1);   // the duplicate must not re-publish
    }

    @Test
    void outboxIsDrainedAndConsumerProcessesTheEventExactlyOnce() {
        var request = new WebhookRequest("evt-3003", "order.shipped", Map.of("orderId", "C-3"));
        var result = ingest.ingest("acme", request);

        // The scheduled poller drains it; the consumer records it as processed.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(outbox.findAll())
                    .filteredOn(m -> m.getAggregateId().equals(result.eventId()))
                    .allMatch(m -> m.getStatus() == OutboxMessage.Status.PUBLISHED);

            assertThat(processed.existsById(result.eventId())).isTrue();

            var synced = downstream.findById(result.eventId());
            assertThat(synced).isPresent();
            assertThat(synced.get().getEventType()).isEqualTo("order.shipped");
        });
    }

    @Test
    void unsupportedEventTypeIsRejectedRatherThanRetried() {
        // "unrecognized.event" is not in WebhookEventConsumer.SUPPORTED_EVENT_TYPES,
        // so this must land in the DLQ, not sit retrying against a message that will
        // never succeed.
        var request = new WebhookRequest("evt-4004", "unrecognized.event", Map.of("x", 1));
        var result = ingest.ingest("acme", request);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(outbox.findAll())
                    .filteredOn(m -> m.getAggregateId().equals(result.eventId()))
                    .allMatch(m -> m.getStatus() == OutboxMessage.Status.PUBLISHED);
        });

        // Give the consumer a moment to run and fail; it should never mark this one
        // processed or synced, since NonRetryableWebhookException goes to the DLQ.
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(downstream.findById(result.eventId())).isEmpty();
        });
    }
}
