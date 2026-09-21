package com.pad.broker.util;

import java.time.Instant;

/** Minimal console logger: timestamp + event name + details, as required for MVP observability. */
public final class BrokerLogger {

    private BrokerLogger() {
    }

    public static void log(String event, String details) {
        System.out.printf("[%s] %s - %s%n", Instant.now(), event, details);
    }

    public static void log(String event) {
        log(event, "");
    }
}
