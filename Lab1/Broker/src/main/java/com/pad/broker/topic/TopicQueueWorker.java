package com.pad.broker.topic;

import com.pad.broker.model.Message;
import com.pad.broker.net.MessageSender;
import com.pad.broker.subscription.MessageStore;
import com.pad.broker.subscription.SubscriberConnectionRegistry;
import com.pad.broker.subscription.SubscriberDeliveryExecutor;
import com.pad.broker.subscription.SubscriptionRegistry;
import com.pad.broker.util.BrokerLogger;

import java.io.IOException;
import java.util.Set;

/** Dedicated consumer for a single topic's queue: pulls one message at a time and fans it out. */
public class TopicQueueWorker implements Runnable {

    private final Topic topic;
    private final SubscriptionRegistry subscriptionRegistry;
    private final MessageStore messageStore;
    private final SubscriberConnectionRegistry connectionRegistry;
    private final SubscriberDeliveryExecutor deliveryExecutor;

    public TopicQueueWorker(Topic topic,
                             SubscriptionRegistry subscriptionRegistry,
                             MessageStore messageStore,
                             SubscriberConnectionRegistry connectionRegistry,
                             SubscriberDeliveryExecutor deliveryExecutor) {
        this.topic = topic;
        this.subscriptionRegistry = subscriptionRegistry;
        this.messageStore = messageStore;
        this.connectionRegistry = connectionRegistry;
        this.deliveryExecutor = deliveryExecutor;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message published = topic.getQueue().take();
                fanOut(published);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void fanOut(Message published) {
        Message outbound = published.asSubscriberMessage();
        Set<String> subscribers = subscriptionRegistry.getSubscribers(topic.getName());
        for (String subscriberId : subscribers) {
            // Per-subscriber executor: isolated from other subscribers, yet FIFO for this one,
            // so ordering within the topic is preserved even under concurrent publishes.
            deliveryExecutor.submit(subscriberId, () -> deliverTo(subscriberId, outbound));
        }
        BrokerLogger.log("Message routed", "messageId=" + published.messageId() + " topic=" + topic.getName() + " subscribers=" + subscribers.size());
    }

    private void deliverTo(String subscriberId, Message outbound) {
        // Persist first, then attempt live delivery. Removal happens only on ack, so a failed
        // send (or no active connection at all) simply leaves the message pending for retransmission.
        messageStore.enqueue(subscriberId, outbound);
        connectionRegistry.get(subscriberId).ifPresent(sender -> trySend(sender, subscriberId, outbound));
    }

    private void trySend(MessageSender sender, String subscriberId, Message outbound) {
        try {
            sender.send(outbound);
        } catch (IOException e) {
            BrokerLogger.log("Live delivery failed, will retry on reconnect", "subscriberId=" + subscriberId + " messageId=" + outbound.messageId());
        }
    }
}
