package com.pad.broker.topic;

import com.pad.broker.model.OperationResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks which publisher owns which topic, enforcing 1:1 publisher↔topic assignment. */
public class PublisherRegistry {

    private final TopicRegistry topicRegistry;
    private final Map<String, String> publisherTopics = new ConcurrentHashMap<>();

    public PublisherRegistry(TopicRegistry topicRegistry) {
        this.topicRegistry = topicRegistry;
    }

    // Registration is infrequent, so a single coarse lock keeps the two checks below
    // (publisherId uniqueness, topic ownership) atomic without extra lock machinery.
    public synchronized OperationResult register(String publisherId, String topic) {
        if (publisherTopics.containsKey(publisherId)) {
            return OperationResult.invalid("Publisher '" + publisherId + "' is already registered");
        }
        Topic topicObj = topicRegistry.getOrCreate(topic);
        if (!topicObj.trySetPublisher(publisherId)) {
            return OperationResult.invalid("Topic '" + topic + "' already has a registered publisher");
        }
        publisherTopics.put(publisherId, topic);
        return OperationResult.valid();
    }

    public boolean isRegistered(String publisherId) {
        return publisherTopics.containsKey(publisherId);
    }

    public boolean isRegisteredForTopic(String publisherId, String topic) {
        return topic.equals(publisherTopics.get(publisherId));
    }
}
