package com.bharad.streamforge.downstream;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@RestController
@RequestMapping("/api/downstream")
public class FailureController {

    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.HEALTHY);
    private final AtomicLong latencyMs = new AtomicLong(0);

    @PostMapping("/mode/{mode}")
    public Map<String, Object> setMode(@PathVariable Mode mode,
                                       @RequestParam(defaultValue = "0") long latencyMs) {
        this.mode.set(mode);
        this.latencyMs.set(Math.max(0, latencyMs));
        return Map.of("mode", this.mode.get(), "latencyMs", this.latencyMs.get());
    }

    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> process() throws InterruptedException {
        if (latencyMs.get() > 0) {
            Thread.sleep(latencyMs.get());
        }

        return switch (mode.get()) {
            case HEALTHY -> ResponseEntity.ok(Map.of("status", "processed"));
            case FAIL_500 -> ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "failed", "reason", "simulated-500"));
            case UNAVAILABLE -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "failed", "reason", "simulated-unavailable"));
        };
    }

    public enum Mode {
        HEALTHY,
        FAIL_500,
        UNAVAILABLE
    }
}
