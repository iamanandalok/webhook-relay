package dev.anandalok.webhookrelay.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

public record WebhookRequest(
        @NotBlank String externalId,
        @NotBlank String eventType,
        @NotNull Map<String, Object> payload) {
}
