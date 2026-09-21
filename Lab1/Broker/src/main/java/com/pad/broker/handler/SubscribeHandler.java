package com.pad.broker.handler;

import com.pad.broker.dispatch.MessageHandler;
import com.pad.broker.dlq.DeadLetterQueue;
import com.pad.broker.model.Message;
import com.pad.broker.model.OperationResult;
import com.pad.broker.net.ClientSession;
import com.pad.broker.subscription.MessageStore;
import com.pad.broker.subscription.SubscriberConnectionRegistry;
import com.pad.broker.subscription.SubscriptionRegistry;
import com.pad.broker.topic.TopicRegistry;
import com.pad.broker.util.BrokerLogger;
import com.pad.broker.validation.MessageValidator;

import java.util.List;

public class SubscribeHandler implements MessageHandler {

    private final MessageValidator validator;
    private final TopicRegistry topicRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final SubscriberConnectionRegistry connectionRegistry;
    private final MessageStore messageStore;
    private final DeadLetterQueue deadLetterQueue;

    public SubscribeHandler(MessageValidator validator,
                             TopicRegistry topicRegistry,
                             SubscriptionRegistry subscriptionRegistry,
                             SubscriberConnectionRegistry connectionRegistry,
                             MessageStore messageStore,
                             DeadLetterQueue deadLetterQueue) {
        this.validator = validator;
        this.topicRegistry = topicRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.connectionRegistry = connectionRegistry;
        this.messageStore = messageStore;
        this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public void handle(String rawLine, Message message, ClientSession session) {
        OperationResult validation = validator.validateSubscribe(message);
        if (!validation.success()) {
            deadLetterQueue.add(rawLine, validation.reason());
            session.send(Message.error(validation.reason()));
            return;
        }

        if (!topicRegistry.exists(message.topic())) {
            session.send(Message.error("Topic '" + message.topic() + "' does not exist"));
            return;
        }

        subscriptionRegistry.subscribe(message.topic(), message.subscriberId());
        connectionRegistry.register(message.subscriberId(), session.sender());
        session.setSubscriberId(message.subscriberId());
        BrokerLogger.log("Subscriber subscribed", "subscriberId=" + message.subscriberId() + " topic=" + message.topic());

        session.send(Message.ackRegistration("Subscribed to topic '" + message.topic() + "'"));

        // Same subscriberId reconnecting: redeliver everything still unacked, in original order.
        List<Message> pending = messageStore.getPending(message.subscriberId());
        for (Message pendingMessage : pending) {
            session.send(pendingMessage);
        }
    }
}
