package dev.anandalok.webhookrelay.outbox;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_message")
public class OutboxMessage {

    public enum Status { PENDING, PUBLISHED, FAILED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(nullable = false)
    private String topic;

    /** Kafka partition key. Same key => same partition => ordering preserved per aggregate. */
    @Column(name = "message_key", nullable = false)
    private String messageKey;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status = Status.PENDING;

    @Column(nullable = false)
    private int attempts = 0;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxMessage() { /* JPA */ }

    public OutboxMessage(UUID aggregateId, String topic, String messageKey, String payload) {
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
    }

    public void markPublished() {
        this.status = Status.PUBLISHED;
        this.publishedAt = Instant.now();
    }

    public void markAttemptFailed(int maxAttempts) {
        this.attempts++;
        if (this.attempts >= maxAttempts) {
            this.status = Status.FAILED;   // stops the poller retrying forever; alert on this
        }
    }

    public Long getId() { return id; }
    public UUID getAggregateId() { return aggregateId; }
    public String getTopic() { return topic; }
    public String getMessageKey() { return messageKey; }
    public String getPayload() { return payload; }
    public Status getStatus() { return status; }
    public int getAttempts() { return attempts; }
}
