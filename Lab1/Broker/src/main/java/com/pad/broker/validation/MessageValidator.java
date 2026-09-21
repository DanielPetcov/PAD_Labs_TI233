package com.pad.broker.validation;

import com.pad.broker.model.Message;
import com.pad.broker.model.OperationResult;

/**
 * Centralized, side-effect-free validation of incoming messages per {@code type}.
 * Only checks structural correctness (required fields present); business rules
 * (duplicate ids, topic ownership, existence) live in the registries/handlers.
 */
public class MessageValidator {

    public OperationResult validateRegisterPublisher(Message message) {
        if (isBlank(message.publisherId())) {
            return OperationResult.invalid("publisherId is required");
        }
        if (isBlank(message.topic())) {
            return OperationResult.invalid("topic is required");
        }
        return OperationResult.valid();
    }

    public OperationResult validatePublish(Message message) {
        if (isBlank(message.messageId())) {
            return OperationResult.invalid("messageId is required");
        }
        if (isBlank(message.publisherId())) {
            return OperationResult.invalid("publisherId is required");
        }
        if (isBlank(message.topic())) {
            return OperationResult.invalid("topic is required");
        }
        if (message.payload() == null) {
            return OperationResult.invalid("payload is required");
        }
        if (isBlank(message.timestamp())) {
            return OperationResult.invalid("timestamp is required");
        }
        return OperationResult.valid();
    }

    public OperationResult validateSubscribe(Message message) {
        if (isBlank(message.subscriberId())) {
            return OperationResult.invalid("subscriberId is required");
        }
        if (isBlank(message.topic())) {
            return OperationResult.invalid("topic is required");
        }
        return OperationResult.valid();
    }

    public OperationResult validateAck(Message message) {
        if (isBlank(message.subscriberId())) {
            return OperationResult.invalid("subscriberId is required");
        }
        if (isBlank(message.messageId())) {
            return OperationResult.invalid("messageId is required");
        }
        return OperationResult.valid();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
