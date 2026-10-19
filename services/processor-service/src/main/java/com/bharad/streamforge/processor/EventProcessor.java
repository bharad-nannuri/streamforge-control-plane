package com.bharad.streamforge.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class EventProcessor {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final RestClient downstream;
    private final Counter processedCounter;
    private final Counter failureCounter;
    private final Counter duplicateCounter;
    private final Counter dlqCounter;
    private final Timer durationTimer;

    public EventProcessor(JdbcTemplate jdbcTemplate,
                          ObjectMapper objectMapper,
                          MeterRegistry registry,
                          @Value("${streamforge.downstream-url:http://localhost:8083}") String downstreamUrl) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.downstream = RestClient.builder().baseUrl(downstreamUrl).build();
        this.processedCounter = Counter.builder("streamforge.events.processed").description("Successfully processed events").register(registry);
        this.failureCounter = Counter.builder("streamforge.events.processing_failures").description("Processing attempts that failed").register(registry);
        this.duplicateCounter = Counter.builder("streamforge.events.duplicate_processing").description("Redelivered events skipped after successful processing").register(registry);
        this.dlqCounter = Counter.builder("streamforge.events.dlq").description("Events arriving on the DLQ").register(registry);
        this.durationTimer = Timer.builder("streamforge.processing.duration").description("End-to-end processor attempt duration").publishPercentileHistogram().register(registry);
    }

    @KafkaListener(topics = "streamforge.events", groupId = "streamforge-processors")
    public void process(String envelope) throws Exception {
        JsonNode event = objectMapper.readTree(envelope);
        String eventId = event.path("eventId").asText();

        String status = jdbcTemplate.query(
                "select status from sf_event where event_id=?",
                rs -> rs.next() ? rs.getString(1) : null,
                eventId);
        if ("PROCESSED".equals(status)) {
            duplicateCounter.increment();
            return;
        }

        jdbcTemplate.update("""
                update sf_event
                   set attempts=attempts+1, status='PROCESSING', last_error=null, updated_at=now()
                 where event_id=?
                """, eventId);

        long start = System.nanoTime();
        try {
            downstream.post()
                    .uri("/api/downstream/process")
                    .body(Map.of("eventId", eventId))
                    .retrieve()
                    .toBodilessEntity();

            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            jdbcTemplate.update("""
                    update sf_event
                       set status='PROCESSED', processed_at=now(), processing_duration_ms=?, last_error=null, updated_at=now()
                     where event_id=?
                    """, durationMs, eventId);
            durationTimer.record(durationMs, TimeUnit.MILLISECONDS);
            processedCounter.increment();
        } catch (Exception e) {
            failureCounter.increment();
            jdbcTemplate.update("""
                    update sf_event
                       set status='RETRYING', last_error=?, updated_at=now()
                     where event_id=?
                    """, abbreviate(e.getMessage()), eventId);
            throw e;
        }
    }

    @KafkaListener(topics = "streamforge.dlq", groupId = "streamforge-dlq-tracker")
    public void trackDlq(String envelope) throws Exception {
        String eventId = objectMapper.readTree(envelope).path("eventId").asText();
        jdbcTemplate.update("""
                update sf_event
                   set status='DLQ', updated_at=now()
                 where event_id=?
                """, eventId);
        dlqCounter.increment();
    }

    private static String abbreviate(String value) {
        if (value == null) return "unknown";
        return value.length() > 500 ? value.substring(0, 500) : value;
    }
}
