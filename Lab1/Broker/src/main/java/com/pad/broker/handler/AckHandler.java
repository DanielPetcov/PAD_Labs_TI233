package com.pad.broker.handler;

import com.pad.broker.dispatch.MessageHandler;
import com.pad.broker.dlq.DeadLetterQueue;
import com.pad.broker.model.Message;
import com.pad.broker.model.OperationResult;
import com.pad.broker.net.ClientSession;
import com.pad.broker.subscription.MessageStore;
import com.pad.broker.util.BrokerLogger;
import com.pad.broker.validation.MessageValidator;

public class AckHandler implements MessageHandler {

    private final MessageValidator validator;
    private final MessageStore messageStore;
    private final DeadLetterQueue deadLetterQueue;

    public AckHandler(MessageValidator validator, MessageStore messageStore, DeadLetterQueue deadLetterQueue) {
        this.validator = validator;
        this.messageStore = messageStore;
        this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public void handle(String rawLine, Message message, ClientSession session) {
        OperationResult validation = validator.validateAck(message);
        if (!validation.success()) {
            deadLetterQueue.add(rawLine, validation.reason());
            session.send(Message.error(validation.reason()));
            return;
        }

        messageStore.ack(message.subscriberId(), message.messageId());
        BrokerLogger.log("Ack received", "subscriberId=" + message.subscriberId() + " messageId=" + message.messageId());
        session.send(Message.ackConfirmed(message.messageId()));
    }
}
