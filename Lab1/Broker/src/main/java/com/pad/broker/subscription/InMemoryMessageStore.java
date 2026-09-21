package com.pad.broker.subscription;

import com.pad.broker.model.Message;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory {@link MessageStore}: one {@link SubscriberQueue} per subscriberId, created on first use. */
public class InMemoryMessageStore implements MessageStore {

    private final Map<String, SubscriberQueue> queuesBySubscriber = new ConcurrentHashMap<>();

    private SubscriberQueue queueFor(String subscriberId) {
        return queuesBySubscriber.computeIfAbsent(subscriberId, id -> new SubscriberQueue());
    }

    @Override
    public void enqueue(String subscriberId, Message message) {
        queueFor(subscriberId).enqueue(message);
    }

    @Override
    public void ack(String subscriberId, String messageId) {
        queueFor(subscriberId).removeAcked(messageId);
    }

    @Override
    public List<Message> getPending(String subscriberId) {
        return queueFor(subscriberId).getPending();
    }
}
