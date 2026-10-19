package com.bharad.streamforge.control;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RestController
@RequestMapping("/api/control")
public class ControlPlaneController {
    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaLagService kafkaLagService;
    private final RestClient ingestion;
    private final RestClient processor;
    private final RestClient downstream;
    private final Counter replayCounter;

    public ControlPlaneController(JdbcTemplate jdbcTemplate,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  KafkaLagService kafkaLagService,
                                  MeterRegistry registry,
                                  @Value("${streamforge.ingestion-url:http://localhost:8081}") String ingestionUrl,
                                  @Value("${streamforge.processor-url:http://localhost:8082}") String processorUrl,
                                  @Value("${streamforge.downstream-url:http://localhost:8083}") String downstreamUrl) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaLagService = kafkaLagService;
        this.ingestion = RestClient.builder().baseUrl(ingestionUrl).build();
        this.processor = RestClient.builder().baseUrl(processorUrl).build();
        this.downstream = RestClient.builder().baseUrl(downstreamUrl).build();
        this.replayCounter = Counter.builder("streamforge.events.replayed").description("Events replayed from DLQ").register(registry);
    }

    @GetMapping("/summary")
    public Map<String, Object> summary() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Map<String, Object> row : jdbcTemplate.queryForList("select status, count(*) as count from sf_event group by status")) {
            counts.put(String.valueOf(row.get("status")), ((Number) row.get("count")).longValue());
        }

        long total = jdbcTemplate.queryForObject("select count(*) from sf_event", Long.class);
        long processedLast10s = jdbcTemplate.queryForObject(
                "select count(*) from sf_event where processed_at >= now() - interval '10 seconds'", Long.class);
        long backlog = jdbcTemplate.queryForObject(
                "select count(*) from sf_event where status in ('ACCEPTED','PROCESSING','RETRYING','REPLAYING')", Long.class);
        Double p95 = jdbcTemplate.queryForObject(
                "select percentile_cont(0.95) within group (order by processing_duration_ms) from sf_event where processing_duration_ms is not null",
                Double.class);

        List<Map<String, Object>> recent = jdbcTemplate.queryForList("""
                select event_id, event_type, status, attempts, replay_count, processing_duration_ms, last_error, updated_at
                  from sf_event
                 order by updated_at desc
                 limit 18
                """);

        Map<String, Object> services = new LinkedHashMap<>();
        services.put("controlPlane", "HEALTHY");
        services.put("ingestion", health(ingestion));
        services.put("processor", health(processor));
        services.put("downstream", health(downstream));
        services.put("kafka", kafkaLagService.healthy() ? "HEALTHY" : "DOWN");
        services.put("postgres", postgresHealthy() ? "HEALTHY" : "DOWN");

        long dlq = counts.getOrDefault("DLQ", 0L);
        long retrying = counts.getOrDefault("RETRYING", 0L);
        long processed = counts.getOrDefault("PROCESSED", 0L);
        double successRate = total == 0 ? 100.0 : (processed * 100.0 / total);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("timestamp", Instant.now().toString());
        response.put("total", total);
        response.put("processed", processed);
        response.put("retrying", retrying);
        response.put("dlq", dlq);
        response.put("backlog", backlog);
        response.put("consumerLag", kafkaLagService.consumerLag());
        response.put("throughputPerSecond", processedLast10s / 10.0);
        response.put("p95LatencyMs", p95 == null ? 0 : Math.round(p95));
        response.put("successRate", Math.round(successRate * 100.0) / 100.0);
        response.put("counts", counts);
        response.put("downstream", safeMode());
        response.put("services", services);
        response.put("recent", recent);
        return response;
    }

    @PostMapping("/traffic")
    public Map<String, Object> traffic(@RequestParam(defaultValue = "100") int count) throws Exception {
        int safeCount = Math.max(1, Math.min(count, 500));
        List<Future<?>> futures = new ArrayList<>(safeCount);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < safeCount; i++) {
                int sequence = i;
                futures.add(executor.submit(() -> ingestion.post()
                        .uri("/api/events")
                        .body(Map.of(
                                "type", "WORKLOAD_EVENT",
                                "payload", "{\"sequence\":" + sequence + ",\"source\":\"control-plane\"}",
                                "idempotencyKey", UUID.randomUUID().toString()))
                        .retrieve()
                        .toBodilessEntity()));
            }
            for (Future<?> future : futures) future.get();
        }
        return Map.of("generated", safeCount, "status", "ACCEPTED");
    }

    @PostMapping("/failure")
    public Object failure(@RequestParam String mode,
                          @RequestParam(defaultValue = "1200") long latencyMs) {
        return downstream.post()
                .uri(uriBuilder -> uriBuilder.path("/api/downstream/mode/{mode}")
                        .queryParam("latencyMs", latencyMs).build(mode))
                .retrieve()
                .body(Object.class);
    }

    @PostMapping("/replay")
    public Map<String, Object> replay(@RequestParam(defaultValue = "200") int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                select event_id, payload from sf_event
                 where status='DLQ'
                 order by updated_at asc
                 limit ?
                """, safeLimit);

        int replayed = 0;
        for (Map<String, Object> row : rows) {
            String eventId = String.valueOf(row.get("event_id"));
            String payload = String.valueOf(row.get("payload"));
            kafkaTemplate.send("streamforge.events", eventId, payload);
            jdbcTemplate.update("""
                    update sf_event
                       set status='REPLAYING', attempts=0, replay_count=replay_count+1, last_error=null, updated_at=now()
                     where event_id=?
                    """, eventId);
            replayCounter.increment();
            replayed++;
        }
        return Map.of("replayed", replayed);
    }

    @PostMapping("/reset")
    public Map<String, Object> reset() {
        downstream.post().uri("/api/downstream/mode/HEALTHY").retrieve().toBodilessEntity();
        int deleted = jdbcTemplate.update("delete from sf_event");
        return Map.of("deleted", deleted, "downstream", "HEALTHY");
    }

    private String health(RestClient client) {
        try {
            ResponseEntity<JsonNode> response = client.get().uri("/actuator/health").retrieve().toEntity(JsonNode.class);
            return response.getStatusCode().is2xxSuccessful() ? "HEALTHY" : "DOWN";
        } catch (Exception e) {
            return "DOWN";
        }
    }

    private boolean postgresHealthy() {
        try {
            Integer one = jdbcTemplate.queryForObject("select 1", Integer.class);
            return one != null && one == 1;
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> safeMode() {
        try {
            Object body = downstream.get().uri("/api/downstream/mode").retrieve().body(Object.class);
            return body instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of("mode", "UNKNOWN");
        } catch (Exception e) {
            return Map.of("mode", "DOWN");
        }
    }
}
