package dev.anandalok.webhookrelay.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.anandalok.webhookrelay.api.WebhookRequest;
import dev.anandalok.webhookrelay.config.RelayProperties;
import dev.anandalok.webhookrelay.domain.WebhookEvent;
import dev.anandalok.webhookrelay.domain.WebhookEventRepository;
import dev.anandalok.webhookrelay.outbox.OutboxMessage;
import dev.anandalok.webhookrelay.outbox.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Service
public class WebhookIngestService {

    private final WebhookEventRepository events;
    private final OutboxRepository outbox;
    private final RelayProperties props;
    private final ObjectMapper json;

    public WebhookIngestService(WebhookEventRepository events,
                                OutboxRepository outbox,
                                RelayProperties props,
                                ObjectMapper json) {
        this.events = events;
        this.outbox = outbox;
        this.props = props;
        this.json = json;
    }

    /**
     * The whole point of this class: the event row and the outbox row commit together
     * or not at all. No Kafka call happens here. If we published inline and the
     * transaction then rolled back, we'd have announced an event that doesn't exist.
     *
     * @return the stored event id, whether newly created or already present
     */
    @Transactional
    public IngestResult ingest(String provider, WebhookRequest request) {
        // Provider redelivery is normal, not exceptional. Treat a repeat as success.
        var existing = events.findByProviderAndExternalId(provider, request.externalId());
        if (existing.isPresent()) {
            return new IngestResult(existing.get().getId(), true);
        }

        UUID id = UUID.randomUUID();
        String payload = writePayload(request.payload());

        events.save(new WebhookEvent(id, provider, request.externalId(), request.eventType(), payload));

        String envelope = writePayload(Map.of(
                "eventId", id.toString(),
                "provider", provider,
                "eventType", request.eventType(),
                "payload", request.payload()));

        // Key by externalId so all events for one entity land on the same partition
        // and keep their relative order.
        outbox.save(new OutboxMessage(id, props.topics().events(), request.externalId(), envelope));

        return new IngestResult(id, false);
    }

    private String writePayload(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Payload is not serializable", e);
        }
    }

    public record IngestResult(UUID eventId, boolean duplicate) {}
}
