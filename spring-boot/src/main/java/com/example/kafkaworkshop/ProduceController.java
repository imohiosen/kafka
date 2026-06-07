package com.example.kafkaworkshop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REST entry point for producing load.
 *
 *   POST /produce?count=N&size=BYTES
 *
 * Sends N JSON messages (each payload padded to ~BYTES) to the workshop topic and
 * returns { "sent": N, "elapsedMs": <time> }. This is the knob you turn to drive
 * the producer during the tuning modules.
 */
@RestController
public class ProduceController {

    private static final Logger log = LoggerFactory.getLogger(ProduceController.class);

    // KafkaTemplate is the auto-configured producer. Key + value are Strings
    // because we serialize the event to a JSON string ourselves (StringSerializer).
    private final KafkaTemplate<String, String> kafka;

    // Jackson turns our WorkshopEvent record into a JSON string.
    private final ObjectMapper json;

    // The topic name, injected from application.yml (workshop.topic).
    private final String topic;

    // Global sequence counter so seq keeps climbing across multiple /produce calls.
    private final AtomicLong seq = new AtomicLong(0);

    public ProduceController(KafkaTemplate<String, String> kafka,
                             ObjectMapper json,
                             @Value("${workshop.topic}") String topic) {
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
    }

    @PostMapping("/produce")
    public Map<String, Object> produce(
            @RequestParam(defaultValue = "1000") int count,
            @RequestParam(defaultValue = "200") int size) throws Exception {

        log.info("Producing {} messages (~{} bytes each) to topic '{}'", count, size, topic);
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            WorkshopEvent event = new WorkshopEvent(
                    UUID.randomUUID().toString(),
                    Instant.now().toString(),       // ISO-8601
                    seq.incrementAndGet(),
                    pad(size)                        // payload padded to ~size bytes
            );
            // Serialize to JSON and send. We don't set a key, so Kafka spreads
            // records across partitions round-robin (good for parallelism here).
            kafka.send(topic, json.writeValueAsString(event));
        }

        // Flush so the elapsed time reflects messages actually handed to the broker,
        // not just buffered locally — important when measuring throughput.
        kafka.flush();

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("Done: sent {} messages in {} ms", count, elapsedMs);
        return Map.of("sent", count, "elapsedMs", elapsedMs);
    }

    /**
     * Build a payload string of roughly {@code size} bytes. Each char is 1 byte in
     * ASCII, so a string of length {@code size} is ~{@code size} bytes of payload.
     */
    private static String pad(int size) {
        int n = Math.max(0, size);
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append('x');
        }
        return sb.toString();
    }
}
