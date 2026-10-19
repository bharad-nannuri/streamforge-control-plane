package com.bharad.streamforge.ingestion;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Counter receivedCounter;

    public EventController(KafkaTemplate<String, String> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.receivedCounter = Counter.builder("streamforge.events.received")
                .description("Total events accepted by the ingestion service")
                .register(meterRegistry);
    }

    @PostMapping
    public ResponseEntity<EventAccepted> publish(@RequestBody EventRequest request) {
        String eventId = UUID.randomUUID().toString();
        String payload = "{\"eventId\":\"" + eventId + "\",\"type\":\"" + request.type()
                + "\",\"payload\":\"" + escape(request.payload()) + "\",\"createdAt\":\""
                + Instant.now() + "\"}";

        kafkaTemplate.send("streamforge.events", eventId, payload);
        receivedCounter.increment();

        return ResponseEntity.accepted().body(new EventAccepted(eventId, "ACCEPTED"));
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public record EventRequest(String type, String payload) {}
    public record EventAccepted(String eventId, String status) {}
}
