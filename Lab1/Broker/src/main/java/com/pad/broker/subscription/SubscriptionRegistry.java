package com.pad.broker.subscription;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/** Thread-safe topic → subscriberId set. Reads (fan-out) are far more frequent than writes (subscribe), hence CopyOnWriteArraySet. */
public class SubscriptionRegistry {

    private final ConcurrentHashMap<String, Set<String>> topicSubscribers = new ConcurrentHashMap<>();

    public void subscribe(String topic, String subscriberId) {
        topicSubscribers.computeIfAbsent(topic, key -> new CopyOnWriteArraySet<>()).add(subscriberId);
    }

    public Set<String> getSubscribers(String topic) {
        return topicSubscribers.getOrDefault(topic, Set.of());
    }
}
