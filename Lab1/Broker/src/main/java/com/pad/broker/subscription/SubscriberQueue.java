package com.pad.broker.subscription;

import com.pad.broker.model.Message;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Persistent (in-memory, for MVP) queue of messages pending delivery to one subscriber.
 * Backed by a LinkedHashMap keyed by messageId so insertion order (== topic publish order)
 * is preserved while still allowing O(1) removal on ack.
 */
public class SubscriberQueue {

    private final LinkedHashMap<String, Message> pending = new LinkedHashMap<>();

    public synchronized void enqueue(Message message) {
        pending.putIfAbsent(message.messageId(), message);
    }

    public synchronized void removeAcked(String messageId) {
        pending.remove(messageId);
    }

    public synchronized List<Message> getPending() {
        return new ArrayList<>(pending.values());
    }
}
