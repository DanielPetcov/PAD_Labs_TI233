package com.pad.broker.topic;

import com.pad.broker.model.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** FIFO queue of published messages awaiting fan-out for a single topic. */
public class TopicQueue {

    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();

    public void enqueue(Message message) {
        queue.add(message);
    }

    public Message take() throws InterruptedException {
        return queue.take();
    }
}
