package com.bharad.streamforge.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventProcessor {

    private final Counter processedCounter;

    public EventProcessor(MeterRegistry meterRegistry) {
        this.processedCounter = Counter.builder("streamforge.events.processed")
                .description("Total events processed successfully")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "streamforge.events", groupId = "streamforge-processors")
    public void process(String event) {
        // v1 bootstrap: downstream calls, idempotency, retry/DLQ handling,
        // and persistence are added in the next implementation slice.
        processedCounter.increment();
        System.out.println("Processed event: " + event);
    }
}
