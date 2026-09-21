package com.pad.broker.dispatch;

import com.pad.broker.dlq.DeadLetterQueue;
import com.pad.broker.model.Message;
import com.pad.broker.net.ClientSession;

import java.util.Map;

/** Routes a decoded {@link Message} to the {@link MessageHandler} registered for its {@code type}. */
public class MessageDispatcher {

    private final Map<String, MessageHandler> handlersByType;
    private final DeadLetterQueue deadLetterQueue;

    public MessageDispatcher(Map<String, MessageHandler> handlersByType, DeadLetterQueue deadLetterQueue) {
        this.handlersByType = handlersByType;
        this.deadLetterQueue = deadLetterQueue;
    }

    public void dispatch(String rawLine, Message message, ClientSession session) {
        MessageHandler handler = handlersByType.get(message.type());
        if (handler == null) {
            String reason = "Unknown message type: " + message.type();
            deadLetterQueue.add(rawLine, reason);
            session.send(Message.error(reason));
            return;
        }
        handler.handle(rawLine, message, session);
    }
}
