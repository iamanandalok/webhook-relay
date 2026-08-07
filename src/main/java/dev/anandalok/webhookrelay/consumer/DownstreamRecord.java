package dev.anandalok.webhookrelay.consumer;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

/**
 * The synced, downstream-facing projection of an event. Separate from
 * {@code webhook_event} on purpose: that table is the raw system of record,
 * this one is what the fan-out target actually consumes.
 */
@Entity
@Table(name = "downstream_record")
public class DownstreamRecord {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "provider", nullable = false)
    private String provider;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "synced_at", nullable = false)
    private Instant syncedAt = Instant.now();

    protected DownstreamRecord() { /* JPA */ }

    public DownstreamRecord(UUID eventId, String provider, String eventType, String payload) {
        this.eventId = eventId;
        this.provider = provider;
        this.eventType = eventType;
        this.payload = payload;
    }

    public UUID getEventId() { return eventId; }
    public String getProvider() { return provider; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public Instant getSyncedAt() { return syncedAt; }
}
