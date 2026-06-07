package com.example.kafkaworkshop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes from the workshop topic and logs throughput so you can SEE the effect
 * of tuning. The point of the workshop is to change a producer/consumer knob and
 * watch the messages/sec number move.
 *
 * Reporting is lock-free and cheap: we just count messages and, roughly every
 * 5 seconds, log the rate since the last report plus a running total.
 */
@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    // Log a throughput line about this often.
    private static final long REPORT_EVERY_MS = 5_000;

    private final AtomicLong total = new AtomicLong(0);          // all messages ever seen
    private final AtomicLong sinceLastReport = new AtomicLong(0); // messages in current window
    private volatile long windowStart = System.currentTimeMillis();

    /**
     * One @KafkaListener instance. Spring's listener container drives poll() for us
     * using the consumer knobs from application.yml (group.id, max.poll.records,
     * fetch.min.bytes, fetch.max.wait.ms, enable.auto.commit, auto.offset.reset).
     *
     * The value is the raw JSON string — we don't deserialize into WorkshopEvent
     * here to keep the hot path cheap, but you could parse it with ObjectMapper.
     */
    @KafkaListener(topics = "${workshop.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String value) {
        total.incrementAndGet();
        sinceLastReport.incrementAndGet();
        maybeReport();
    }

    /**
     * If at least REPORT_EVERY_MS has passed, log msgs/sec for the window and reset
     * the window. Synchronized so two listener threads don't double-log; the work is
     * trivial so contention is negligible.
     */
    private synchronized void maybeReport() {
        long now = System.currentTimeMillis();
        long elapsed = now - windowStart;
        if (elapsed < REPORT_EVERY_MS) {
            return;
        }
        long count = sinceLastReport.getAndSet(0);
        double perSec = elapsed > 0 ? (count * 1000.0 / elapsed) : 0.0;
        log.info("consumed {} msg in {} ms = {} msg/sec | total {}",
                count, elapsed, String.format("%.0f", perSec), total.get());
        windowStart = now;
    }
}
