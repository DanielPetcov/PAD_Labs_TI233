package com.pad.broker.dlq;

import com.pad.broker.util.BrokerLogger;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Isolates invalid or rejected messages instead of dropping them silently. The original message
 * is kept as the raw JSON line, never as a parsed {@code Message} — one rejection reason is
 * "invalid JSON", which by definition can't be represented as a parsed object.
 */
public class DeadLetterQueue {

    private final ConcurrentLinkedQueue<DeadLetterEntry> entries = new ConcurrentLinkedQueue<>();

    public void add(String originalMessage, String reason) {
        DeadLetterEntry entry = new DeadLetterEntry(
                UUID.randomUUID().toString(), originalMessage, reason, Instant.now());
        entries.add(entry);
        BrokerLogger.log("[DLQ] Message rejected", "reason=" + reason + " message=" + originalMessage);
    }

    public List<DeadLetterEntry> getAll() {
        return Collections.unmodifiableList(List.copyOf(entries));
    }

    public int size() {
        return entries.size();
    }
}
