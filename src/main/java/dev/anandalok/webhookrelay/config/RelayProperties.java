package dev.anandalok.webhookrelay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "relay")
public record RelayProperties(Topics topics, Outbox outbox, Retry retry, Hmac hmac) {

    public record Topics(String events, String dlq) {}

    public record Outbox(long pollIntervalMs, int batchSize) {}

    public record Retry(int maxAttempts, long initialBackoffMs, double multiplier) {}

    /**
     * One HMAC secret per provider, keyed by the {provider} path segment. Fail
     * closed: a provider with no entry here has every request rejected, not
     * waved through. An unregistered provider name must never be a bypass.
     */
    public record Hmac(Map<String, String> secrets) {
        public Hmac {
            secrets = secrets == null ? Map.of() : secrets;
        }
    }
}
