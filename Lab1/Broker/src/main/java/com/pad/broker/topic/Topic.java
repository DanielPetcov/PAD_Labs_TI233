package com.pad.broker.topic;

/** A topic: the single publisher allowed to write to it, plus its pending-message queue. */
public class Topic {

    private final String name;
    private final TopicQueue queue = new TopicQueue();
    private volatile String publisherId;

    public Topic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public TopicQueue getQueue() {
        return queue;
    }

    public String getPublisherId() {
        return publisherId;
    }

    /** Atomically claims this topic for a publisher; returns false if it already has one. */
    public synchronized boolean trySetPublisher(String publisherId) {
        if (this.publisherId != null) {
            return false;
        }
        this.publisherId = publisherId;
        return true;
    }
}
