package com.pad.broker.subscription;

import com.pad.broker.net.MessageSender;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Maps a subscriberId to its currently-connected socket, when connected. Absence means "offline". */
public class SubscriberConnectionRegistry {

    private final Map<String, MessageSender> connections = new ConcurrentHashMap<>();

    public void register(String subscriberId, MessageSender sender) {
        connections.put(subscriberId, sender);
    }

    /** Removes the mapping only if it still points at this exact connection, so a fresh reconnect is never evicted by a stale disconnect. */
    public void unregister(String subscriberId, MessageSender sender) {
        connections.remove(subscriberId, sender);
    }

    public Optional<MessageSender> get(String subscriberId) {
        return Optional.ofNullable(connections.get(subscriberId));
    }
}
