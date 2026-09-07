package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Phase of an assistant message.
 */
public enum MessagePhase {
    COMMENTARY("commentary"),
    FINAL_ANSWER("final_answer");

    private final String value;

    MessagePhase(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static MessagePhase fromValue(String value) {
        if (value == null) {
            return FINAL_ANSWER;
        }
        for (MessagePhase phase : values()) {
            if (phase.value.equalsIgnoreCase(value)) {
                return phase;
            }
        }
        return FINAL_ANSWER;
    }
}
