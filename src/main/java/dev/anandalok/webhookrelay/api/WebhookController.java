package dev.anandalok.webhookrelay.api;

import dev.anandalok.webhookrelay.ingest.WebhookIngestService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/webhooks")
public class WebhookController {

    private final WebhookIngestService ingest;

    public WebhookController(WebhookIngestService ingest) {
        this.ingest = ingest;
    }

    /**
     * Returns as soon as the event is durably stored. Processing happens off the
     * request thread. Webhook providers time out fast and retry aggressively, so
     * doing the work inline would turn a slow downstream into duplicate deliveries.
     *
     * By the time a request reaches here, {@link HmacSignatureFilter} has already
     * verified the signature. The controller trusts what it's handed; it doesn't
     * re-derive trust decisions the edge already made.
     */
    @PostMapping("/{provider}")
    public ResponseEntity<Map<String, Object>> receive(
            @PathVariable String provider,
            @Valid @RequestBody WebhookRequest request) {

        var result = ingest.ingest(provider, request);

        return ResponseEntity
                .status(result.duplicate() ? HttpStatus.OK : HttpStatus.ACCEPTED)
                .body(Map.of("eventId", result.eventId(), "duplicate", result.duplicate()));
    }
}
