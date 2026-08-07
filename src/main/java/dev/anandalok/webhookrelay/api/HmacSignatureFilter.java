package dev.anandalok.webhookrelay.api;

import dev.anandalok.webhookrelay.config.RelayProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verifies X-Webhook-Signature (hex HMAC-SHA256 over the raw request body) before
 * the request reaches the controller. Rejecting bad signatures belongs at the edge,
 * not buried in service logic where it's easy to forget on the next endpoint.
 *
 * Runs before Spring Security or any framework-level auth would in a fuller build --
 * this endpoint has none, which is a deliberate, documented gap (see README).
 */
public class HmacSignatureFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(HmacSignatureFilter.class);
    private static final String SIGNATURE_HEADER = "X-Webhook-Signature";
    private static final Pattern PROVIDER_PATH = Pattern.compile("^/api/v1/webhooks/([^/]+)$");
    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    private final RelayProperties props;

    public HmacSignatureFilter(RelayProperties props) {
        this.props = props;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !PATH_MATCHER.match("/api/v1/webhooks/*", request.getRequestURI());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                     HttpServletResponse response,
                                     FilterChain chain) throws ServletException, IOException {

        String provider = extractProvider(request.getRequestURI());
        String secret = provider == null ? null : props.hmac().secrets().get(provider);

        if (secret == null) {
            log.warn("Rejecting webhook for provider '{}': no HMAC secret configured", provider);
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unknown provider");
            return;
        }

        String signatureHeader = request.getHeader(SIGNATURE_HEADER);
        if (signatureHeader == null || signatureHeader.isBlank()) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing " + SIGNATURE_HEADER);
            return;
        }

        var cachedRequest = new CachedBodyHttpServletRequest(request);
        String expected = hmacHex(secret, cachedRequest.getBody());

        // Constant-time comparison: a timing difference between "wrong at byte 0" and
        // "wrong at byte 31" is a side channel an attacker can use to brute-force the
        // signature one byte at a time.
        if (!constantTimeEquals(expected, signatureHeader.trim())) {
            log.warn("Rejecting webhook for provider '{}': signature mismatch", provider);
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid signature");
            return;
        }

        chain.doFilter(cachedRequest, response);
    }

    private String extractProvider(String uri) {
        Matcher matcher = PROVIDER_PATH.matcher(uri);
        return matcher.matches() ? matcher.group(1) : null;
    }

    private String hmacHex(String secret, byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(body));
        } catch (NoSuchAlgorithmException | java.security.InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    private boolean constantTimeEquals(String expectedHex, String providedHex) {
        try {
            return MessageDigest.isEqual(
                    HexFormat.of().parseHex(expectedHex),
                    HexFormat.of().parseHex(providedHex));
        } catch (IllegalArgumentException notHex) {
            return false;
        }
    }
}
