package com.pad.broker.handler;

import com.pad.broker.dispatch.MessageHandler;
import com.pad.broker.dlq.DeadLetterQueue;
import com.pad.broker.model.Message;
import com.pad.broker.model.OperationResult;
import com.pad.broker.net.ClientSession;
import com.pad.broker.topic.PublisherRegistry;
import com.pad.broker.topic.Topic;
import com.pad.broker.topic.TopicRegistry;
import com.pad.broker.util.BrokerLogger;
import com.pad.broker.validation.MessageValidator;

import java.util.Optional;

public class PublishHandler implements MessageHandler {

    private final MessageValidator validator;
    private final PublisherRegistry publisherRegistry;
    private final TopicRegistry topicRegistry;
    private final DeadLetterQueue deadLetterQueue;

    public PublishHandler(MessageValidator validator,
                           PublisherRegistry publisherRegistry,
                           TopicRegistry topicRegistry,
                           DeadLetterQueue deadLetterQueue) {
        this.validator = validator;
        this.publisherRegistry = publisherRegistry;
        this.topicRegistry = topicRegistry;
        this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public void handle(String rawLine, Message message, ClientSession session) {
        OperationResult validation = validator.validatePublish(message);
        if (!validation.success()) {
            reject(rawLine, session, validation.reason());
            return;
        }

        if (!publisherRegistry.isRegistered(message.publisherId())) {
            reject(rawLine, session, "Publisher '" + message.publisherId() + "' is not registered");
            return;
        }

        if (!publisherRegistry.isRegisteredForTopic(message.publisherId(), message.topic())) {
            reject(rawLine, session, "Publisher '" + message.publisherId() + "' is not registered for topic '" + message.topic() + "'");
            return;
        }

        Optional<Topic> topicOpt = topicRegistry.find(message.topic());
        if (topicOpt.isEmpty()) {
            reject(rawLine, session, "Topic '" + message.topic() + "' does not exist");
            return;
        }

        topicOpt.get().getQueue().enqueue(message);
        BrokerLogger.log("Message received", "messageId=" + message.messageId() + " topic=" + message.topic());
        session.send(Message.publishAck(message.messageId(), message.topic()));
    }

    private void reject(String rawLine, ClientSession session, String reason) {
        deadLetterQueue.add(rawLine, reason);
        session.send(Message.error(reason));
    }
}
