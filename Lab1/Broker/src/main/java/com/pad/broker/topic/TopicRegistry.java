package com.pad.broker.topic;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** Thread-safe topic → {@link Topic} mapping with dynamic, race-free topic creation. */
public class TopicRegistry {

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final Consumer<Topic> onTopicCreated;

    public TopicRegistry(Consumer<Topic> onTopicCreated) {
        this.onTopicCreated = onTopicCreated;
    }

    public Topic getOrCreate(String name) {
        boolean[] created = {false};
        // computeIfAbsent is atomic per key: only one thread ever constructs the Topic for a
        // given name, even under concurrent register_publisher calls for the same topic.
        Topic topic = topics.computeIfAbsent(name, key -> {
            created[0] = true;
            return new Topic(key);
        });
        if (created[0]) {
            onTopicCreated.accept(topic);
        }
        return topic;
    }

    public Optional<Topic> find(String name) {
        return Optional.ofNullable(topics.get(name));
    }

    public boolean exists(String name) {
        return topics.containsKey(name);
    }
}
