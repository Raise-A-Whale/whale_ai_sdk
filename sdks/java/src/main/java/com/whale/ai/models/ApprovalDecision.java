package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Decision for Human-In-The-Loop approval.
 */
public enum ApprovalDecision {
    APPROVE("approve"),
    REJECT("reject");

    private final String value;

    ApprovalDecision(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static ApprovalDecision fromValue(String value) {
        if (value == null) {
            return APPROVE;
        }
        for (ApprovalDecision decision : values()) {
            if (decision.value.equalsIgnoreCase(value)) {
                return decision;
            }
        }
        throw new IllegalArgumentException("Unknown approval decision: " + value);
    }
}
