package dev.anandalok.webhookrelay.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "webhook_event")
public class WebhookEvent {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String provider;

    /** The provider's own id for this event. Used to reject duplicate deliveries. */
    @Column(name = "external_id", nullable = false)
    private String externalId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected WebhookEvent() { /* JPA */ }

    public WebhookEvent(UUID id, String provider, String externalId, String eventType, String payload) {
        this.id = id;
        this.provider = provider;
        this.externalId = externalId;
        this.eventType = eventType;
        this.payload = payload;
        this.receivedAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getProvider() { return provider; }
    public String getExternalId() { return externalId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public Instant getReceivedAt() { return receivedAt; }
}
