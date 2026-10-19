package com.bharad.streamforge.downstream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@RestController
@RequestMapping("/api/downstream")
public class FailureController {
    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.HEALTHY);
    private final AtomicLong latencyMs = new AtomicLong(1_200);
    private final Counter requests;
    private final Counter failures;

    public FailureController(MeterRegistry registry) {
        this.requests = Counter.builder("streamforge.downstream.requests").register(registry);
        this.failures = Counter.builder("streamforge.downstream.failures").register(registry);
    }

    @GetMapping("/mode")
    public Map<String, Object> mode() {
        return Map.of("mode", mode.get().name(), "latencyMs", latencyMs.get());
    }

    @PostMapping("/mode/{newMode}")
    public Map<String, Object> setMode(@PathVariable Mode newMode,
                                       @RequestParam(defaultValue = "1200") long latencyMs) {
        mode.set(newMode);
        this.latencyMs.set(Math.max(0, Math.min(latencyMs, 10_000)));
        return mode();
    }

    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> process(@RequestBody(required = false) Map<String, Object> payload) throws InterruptedException {
        requests.increment();
        Mode current = mode.get();

        if (current == Mode.LATENCY) {
            Thread.sleep(latencyMs.get());
            return ResponseEntity.ok(Map.of("status", "processed", "mode", current.name(), "latencyMs", latencyMs.get()));
        }

        if (current == Mode.CHAOS) {
            if (ThreadLocalRandom.current().nextInt(100) < 40) {
                failures.increment();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(Map.of("status", "failed", "mode", current.name(), "reason", "simulated-intermittent-error"));
            }
            Thread.sleep(Math.min(latencyMs.get(), 800));
            return ResponseEntity.ok(Map.of("status", "processed", "mode", current.name()));
        }

        if (current == Mode.FAIL_500) {
            failures.increment();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "failed", "mode", current.name(), "reason", "simulated-500"));
        }

        if (current == Mode.UNAVAILABLE) {
            failures.increment();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "failed", "mode", current.name(), "reason", "simulated-unavailable"));
        }

        return ResponseEntity.ok(Map.of("status", "processed", "mode", current.name()));
    }

    public enum Mode { HEALTHY, LATENCY, CHAOS, FAIL_500, UNAVAILABLE }
}
