package com.example.kafkaworkshop;

/**
 * The message we put on the topic, serialized to JSON.
 *
 * The JSON shape is SHARED with the NestJS app and the benchmarks — do not change
 * the field names:
 *
 *   { "id": "<uuid>", "createdAt": "<ISO-8601 string>", "seq": <long>, "payload": "<string>" }
 *
 * A Java record gives us a tidy immutable holder; Jackson serializes the
 * components to JSON fields of the same name.
 */
public record WorkshopEvent(
        String id,        // unique message id (UUID string)
        String createdAt, // ISO-8601 timestamp, e.g. 2026-06-07T12:34:56.789Z
        long seq,         // monotonically increasing sequence number
        String payload    // arbitrary string body, padded to the requested size
) {
}
