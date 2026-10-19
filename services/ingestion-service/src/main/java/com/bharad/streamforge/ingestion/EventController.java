package com.bharad.streamforge.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/events")
public class EventController {
    private static final String TOPIC = "streamforge.events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final Counter acceptedCounter;
    private final Counter duplicateCounter;
    private final Counter publishFailureCounter;

    public EventController(KafkaTemplate<String, String> kafkaTemplate,
                           JdbcTemplate jdbcTemplate,
                           ObjectMapper objectMapper,
                           MeterRegistry registry) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.acceptedCounter = Counter.builder("streamforge.events.accepted").description("Events accepted for processing").register(registry);
        this.duplicateCounter = Counter.builder("streamforge.events.duplicate_ingestion").description("Duplicate idempotency keys rejected at ingestion").register(registry);
        this.publishFailureCounter = Counter.builder("streamforge.events.publish_failures").description("Kafka publish failures").register(registry);
    }

    @PostMapping
    public ResponseEntity<EventAccepted> publish(@RequestBody EventRequest request) throws Exception {
        String eventId = UUID.randomUUID().toString();
        String idempotencyKey = blankToNull(request.idempotencyKey()) == null ? eventId : request.idempotencyKey().trim();
        String eventType = blankToNull(request.type()) == null ? "DEMO_EVENT" : request.type().trim();
        String payload = request.payload() == null ? "{}" : request.payload();

        String existing = jdbcTemplate.query(
                "select event_id from sf_event where idempotency_key = ?",
                rs -> rs.next() ? rs.getString(1) : null,
                idempotencyKey);
        if (existing != null) {
            duplicateCounter.increment();
            return ResponseEntity.ok(new EventAccepted(existing, "DUPLICATE", idempotencyKey));
        }

        String envelope = envelope(eventId, idempotencyKey, eventType, payload);
        jdbcTemplate.update("""
                insert into sf_event(event_id, idempotency_key, event_type, payload, status, attempts, replay_count, created_at, updated_at)
                values (?, ?, ?, ?, 'ACCEPTED', 0, 0, now(), now())
                """, eventId, idempotencyKey, eventType, envelope);

        try {
            kafkaTemplate.send(TOPIC, eventId, envelope).get(5, TimeUnit.SECONDS);
            acceptedCounter.increment();
            return ResponseEntity.accepted().body(new EventAccepted(eventId, "ACCEPTED", idempotencyKey));
        } catch (Exception e) {
            publishFailureCounter.increment();
            jdbcTemplate.update("update sf_event set status='PUBLISH_FAILED', last_error=?, updated_at=now() where event_id=?",
                    abbreviate(e.getMessage()), eventId);
            throw e;
        }
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String eventId) {
        var rows = jdbcTemplate.queryForList("""
                select event_id, idempotency_key, event_type, status, attempts, replay_count, created_at, updated_at,
                       processed_at, processing_duration_ms, last_error
                from sf_event where event_id=?
                """, eventId);
        return rows.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(rows.getFirst());
    }

    private String envelope(String eventId, String idempotencyKey, String eventType, String payload) throws JsonProcessingException {
        return objectMapper.writeValueAsString(Map.of(
                "eventId", eventId,
                "idempotencyKey", idempotencyKey,
                "type", eventType,
                "payload", payload,
                "createdAt", Instant.now().toString()));
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String abbreviate(String value) {
        if (value == null) return "unknown";
        return value.length() > 500 ? value.substring(0, 500) : value;
    }

    public record EventRequest(String type, String payload, String idempotencyKey) {}
    public record EventAccepted(String eventId, String status, String idempotencyKey) {}
}
