package com.pad.broker.subscription;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * One single-threaded executor per subscriberId, created lazily. Delivery to a given
 * subscriber therefore stays strictly FIFO (preserving publish order), while different
 * subscribers run fully in parallel, so a slow or unreachable subscriber can never stall
 * delivery to the rest.
 */
public class SubscriberDeliveryExecutor {

    private final Map<String, ExecutorService> executorsBySubscriber = new ConcurrentHashMap<>();

    public void submit(String subscriberId, Runnable task) {
        executorsBySubscriber
                .computeIfAbsent(subscriberId, id -> Executors.newSingleThreadExecutor())
                .submit(task);
    }
}
