package dev.anandalok.webhookrelay.outbox;

import dev.anandalok.webhookrelay.config.RelayProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Drains the outbox table to Kafka.
 *
 * Deliberately at-least-once: if the send succeeds but this transaction fails to commit
 * the status flip, the row stays PENDING and gets republished. That is why the consumer
 * side is idempotent. The alternative (mark published first) would be at-most-once and
 * could silently drop events, which is the worse failure for this system.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final RelayProperties props;
    private final Counter published;
    private final Counter failed;

    public OutboxPublisher(OutboxRepository repository,
                           KafkaTemplate<String, String> kafka,
                           RelayProperties props,
                           MeterRegistry meters) {
        this.repository = repository;
        this.kafka = kafka;
        this.props = props;
        this.published = Counter.builder("relay.outbox.published").register(meters);
        this.failed = Counter.builder("relay.outbox.failed").register(meters);
    }

    @Scheduled(fixedDelayString = "${relay.outbox.poll-interval-ms}")
    @Transactional
    public void drain() {
        List<OutboxMessage> batch = repository.lockPendingBatch(
                OutboxMessage.Status.PENDING,
                PageRequest.of(0, props.outbox().batchSize()));

        if (batch.isEmpty()) {
            return;
        }

        for (OutboxMessage message : batch) {
            try {
                kafka.send(message.getTopic(), message.getMessageKey(), message.getPayload())
                     .get();   // block: we must know it landed before flipping status
                message.markPublished();
                published.increment();
            } catch (Exception e) {
                message.markAttemptFailed(props.retry().maxAttempts());
                failed.increment();
                log.warn("Outbox publish failed for id={} attempt={}",
                         message.getId(), message.getAttempts(), e);
            }
        }
        repository.saveAll(batch);
    }
}
