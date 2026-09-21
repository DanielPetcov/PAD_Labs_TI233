package com.pad.broker.dispatch;

import com.pad.broker.model.Message;
import com.pad.broker.net.ClientSession;

/** Strategy interface: one implementation per NDJSON message {@code type}. */
public interface MessageHandler {
    /** {@code rawLine} is the exact JSON the client sent, kept around solely for faithful DLQ entries. */
    void handle(String rawLine, Message message, ClientSession session);
}
