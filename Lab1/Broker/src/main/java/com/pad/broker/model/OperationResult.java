package com.pad.broker.model;

/** Generic success/failure outcome with a descriptive reason, used by validators and registries alike. */
public record OperationResult(boolean success, String reason) {

    public static OperationResult valid() {
        return new OperationResult(true, null);
    }

    public static OperationResult invalid(String reason) {
        return new OperationResult(false, reason);
    }
}
