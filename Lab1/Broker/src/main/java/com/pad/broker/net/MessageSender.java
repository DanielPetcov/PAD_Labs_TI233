package com.pad.broker.net;

import com.pad.broker.model.Message;

import java.io.IOException;

/** Abstraction over "write this message to a connected client", isolating socket I/O from business logic. */
public interface MessageSender {
    void send(Message message) throws IOException;
}
