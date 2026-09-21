package com.pad.broker.topic;

import com.pad.broker.subscription.MessageStore;
import com.pad.broker.subscription.SubscriberConnectionRegistry;
import com.pad.broker.subscription.SubscriberDeliveryExecutor;
import com.pad.broker.subscription.SubscriptionRegistry;
import com.pad.broker.util.BrokerLogger;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Bridges {@link TopicRegistry}'s creation callback to spinning up a {@link TopicQueueWorker}.
 * Kept separate so TopicRegistry itself stays a plain map and knows nothing about threads.
 */
public class TopicWorkerLauncher implements Consumer<Topic> {

    private final ExecutorService topicWorkerExecutor;
    private final SubscriptionRegistry subscriptionRegistry;
    private final MessageStore messageStore;
    private final SubscriberConnectionRegistry connectionRegistry;
    private final SubscriberDeliveryExecutor deliveryExecutor;

    public TopicWorkerLauncher(ExecutorService topicWorkerExecutor,
                                SubscriptionRegistry subscriptionRegistry,
                                MessageStore messageStore,
                                SubscriberConnectionRegistry connectionRegistry,
                                SubscriberDeliveryExecutor deliveryExecutor) {
        this.topicWorkerExecutor = topicWorkerExecutor;
        this.subscriptionRegistry = subscriptionRegistry;
        this.messageStore = messageStore;
        this.connectionRegistry = connectionRegistry;
        this.deliveryExecutor = deliveryExecutor;
    }

    @Override
    public void accept(Topic topic) {
        BrokerLogger.log("Topic created", "topic=" + topic.getName());
        topicWorkerExecutor.submit(new TopicQueueWorker(topic, subscriptionRegistry, messageStore, connectionRegistry, deliveryExecutor));
    }
}
