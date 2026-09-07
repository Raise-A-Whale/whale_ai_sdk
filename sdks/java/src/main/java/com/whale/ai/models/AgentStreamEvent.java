package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Real-time streaming event emitted during agent turn processing.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AgentStreamEvent {
    @JsonProperty("type")
    private String type; // e.g. "turn_started", "text_delta", "item_completed", "turn_completed", etc.

    @JsonProperty("turn_id")
    private String turnId;

    @JsonProperty("thread_id")
    private String threadId;

    @JsonProperty("item_id")
    private String itemId;

    @JsonProperty("item_type")
    private String itemType;

    @JsonProperty("phase")
    private MessagePhase phase;

    @JsonProperty("delta")
    private String delta;

    @JsonProperty("signature")
    private String signature;

    @JsonProperty("call_id")
    private String callId;

    @JsonProperty("item")
    private CanonicalItem item;

    @JsonProperty("request_id")
    private String requestId;

    @JsonProperty("tool_call")
    private CanonicalItem toolCall;

    @JsonProperty("reason")
    private String reason;

    @JsonProperty("usage")
    private UsageMetrics usage;

    @JsonProperty("error_code")
    private String errorCode;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("raw")
    private JsonNode raw;

    public AgentStreamEvent() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTurnId() {
        return turnId;
    }

    public void setTurnId(String turnId) {
        this.turnId = turnId;
    }

    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public MessagePhase getPhase() {
        return phase;
    }

    public void setPhase(MessagePhase phase) {
        this.phase = phase;
    }

    public String getDelta() {
        return delta;
    }

    public void setDelta(String delta) {
        this.delta = delta;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getCallId() {
        return callId;
    }

    public void setCallId(String callId) {
        this.callId = callId;
    }

    public CanonicalItem getItem() {
        return item;
    }

    public void setItem(CanonicalItem item) {
        this.item = item;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public CanonicalItem getToolCall() {
        return toolCall;
    }

    public void setToolCall(CanonicalItem toolCall) {
        this.toolCall = toolCall;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public UsageMetrics getUsage() {
        return usage;
    }

    public void setUsage(UsageMetrics usage) {
        this.usage = usage;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public JsonNode getRaw() {
        return raw;
    }

    public void setRaw(JsonNode raw) {
        this.raw = raw;
    }

    @Override
    public String toString() {
        return "AgentStreamEvent{" +
                "type='" + type + '\'' +
                ", turnId='" + turnId + '\'' +
                ", delta='" + delta + '\'' +
                '}';
    }
}
