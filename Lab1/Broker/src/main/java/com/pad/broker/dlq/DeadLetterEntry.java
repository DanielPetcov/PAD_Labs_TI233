package com.pad.broker.dlq;

import java.time.Instant;

/** A rejected message, stored as the raw JSON it arrived as, together with why and when it was rejected. */
public record DeadLetterEntry(
        String deadLetterId,
        String originalMessage,
        String reason,
        Instant failedAt
) {
}
