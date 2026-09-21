package com.pad.broker.net;

import com.pad.broker.dispatch.MessageDispatcher;
import com.pad.broker.dlq.DeadLetterQueue;
import com.pad.broker.subscription.SubscriberConnectionRegistry;
import com.pad.broker.util.BrokerLogger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Accepts TCP connections and hands each one off to its own {@link ClientConnectionHandler}; contains no business logic. */
public class BrokerServer {

    private final int port;
    private final MessageDispatcher dispatcher;
    private final JsonCodec jsonCodec;
    private final SubscriberConnectionRegistry connectionRegistry;
    private final DeadLetterQueue deadLetterQueue;
    private final ExecutorService connectionExecutor = Executors.newCachedThreadPool();

    public BrokerServer(int port,
                         MessageDispatcher dispatcher,
                         JsonCodec jsonCodec,
                         SubscriberConnectionRegistry connectionRegistry,
                         DeadLetterQueue deadLetterQueue) {
        this.port = port;
        this.dispatcher = dispatcher;
        this.jsonCodec = jsonCodec;
        this.connectionRegistry = connectionRegistry;
        this.deadLetterQueue = deadLetterQueue;
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            BrokerLogger.log("Broker started", "port=" + port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = serverSocket.accept();
                BrokerLogger.log("New connection", String.valueOf(socket.getRemoteSocketAddress()));
                ClientConnectionHandler handler = new ClientConnectionHandler(socket, dispatcher, jsonCodec, connectionRegistry, deadLetterQueue);
                connectionExecutor.submit(handler);
            }
        }
    }
}
