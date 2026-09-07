package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Status of an agent turn.
 */
public enum TurnStatus {
    COMPLETED("completed"),
    FAILED("failed"),
    INTERRUPTED("interrupted"),
    REQUIRES_APPROVAL("requires_approval");

    private final String value;

    TurnStatus(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static TurnStatus fromValue(String value) {
        if (value == null) {
            return COMPLETED;
        }
        for (TurnStatus status : values()) {
            if (status.value.equalsIgnoreCase(value)) {
                return status;
            }
        }
        return COMPLETED;
    }
}
