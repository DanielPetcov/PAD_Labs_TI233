package com.pad.broker.subscription;

import com.pad.broker.model.Message;

import java.util.List;

/**
 * Storage abstraction for per-subscriber pending messages. Kept separate from
 * {@link SubscriberQueue} so the in-memory MVP implementation can later be swapped
 * for a disk/SQLite-backed one without touching dispatch or handler code.
 */
public interface MessageStore {

    void enqueue(String subscriberId, Message message);

    void ack(String subscriberId, String messageId);

    List<Message> getPending(String subscriberId);
}
