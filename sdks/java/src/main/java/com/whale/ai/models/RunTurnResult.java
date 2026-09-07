package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Final result of an agent turn.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RunTurnResult {
    @JsonProperty("turn_id")
    private String turnId;

    @JsonProperty("thread_id")
    private String threadId;

    @JsonProperty("status")
    private TurnStatus status;

    @JsonProperty("items")
    private List<CanonicalItem> items;

    @JsonProperty("usage")
    private UsageMetrics usage;

    public RunTurnResult() {
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

    public TurnStatus getStatus() {
        return status;
    }

    public void setStatus(TurnStatus status) {
        this.status = status;
    }

    public List<CanonicalItem> getItems() {
        return items;
    }

    public void setItems(List<CanonicalItem> items) {
        this.items = items;
    }

    public UsageMetrics getUsage() {
        return usage;
    }

    public void setUsage(UsageMetrics usage) {
        this.usage = usage;
    }

    @Override
    public String toString() {
        return "RunTurnResult{" +
                "turnId='" + turnId + '\'' +
                ", threadId='" + threadId + '\'' +
                ", status=" + status +
                ", items=" + (items != null ? items.size() : 0) +
                ", usage=" + usage +
                '}';
    }
}
