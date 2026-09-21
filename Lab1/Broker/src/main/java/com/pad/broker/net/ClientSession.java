package com.pad.broker.net;

import com.pad.broker.model.Message;
import com.pad.broker.util.BrokerLogger;

import java.io.IOException;

/** Per-connection identity (publisherId/subscriberId once known) plus a convenience send. */
public class ClientSession {

    private final MessageSender sender;
    private volatile String publisherId;
    private volatile String subscriberId;

    public ClientSession(MessageSender sender) {
        this.sender = sender;
    }

    public MessageSender sender() {
        return sender;
    }

    public void send(Message message) {
        try {
            sender.send(message);
        } catch (IOException e) {
            BrokerLogger.log("Failed to send to client", e.getMessage());
        }
    }

    public String getPublisherId() {
        return publisherId;
    }

    public void setPublisherId(String publisherId) {
        this.publisherId = publisherId;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(String subscriberId) {
        this.subscriberId = subscriberId;
    }
}
