package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Token usage metrics for LLM generation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UsageMetrics {
    @JsonProperty("input_tokens")
    private long inputTokens;

    @JsonProperty("output_tokens")
    private long outputTokens;

    @JsonProperty("reasoning_tokens")
    private long reasoningTokens;

    @JsonProperty("cache_creation_input_tokens")
    private long cacheCreationInputTokens;

    @JsonProperty("cache_read_input_tokens")
    private long cacheReadInputTokens;

    public UsageMetrics() {
    }

    public UsageMetrics(long inputTokens, long outputTokens) {
        this.inputTokens = inputTokens;
        this.outputTokens = outputTokens;
    }

    public long getInputTokens() {
        return inputTokens;
    }

    public void setInputTokens(long inputTokens) {
        this.inputTokens = inputTokens;
    }

    public long getOutputTokens() {
        return outputTokens;
    }

    public void setOutputTokens(long outputTokens) {
        this.outputTokens = outputTokens;
    }

    public long getReasoningTokens() {
        return reasoningTokens;
    }

    public void setReasoningTokens(long reasoningTokens) {
        this.reasoningTokens = reasoningTokens;
    }

    public long getCacheCreationInputTokens() {
        return cacheCreationInputTokens;
    }

    public void setCacheCreationInputTokens(long cacheCreationInputTokens) {
        this.cacheCreationInputTokens = cacheCreationInputTokens;
    }

    public long getCacheReadInputTokens() {
        return cacheReadInputTokens;
    }

    public void setCacheReadInputTokens(long cacheReadInputTokens) {
        this.cacheReadInputTokens = cacheReadInputTokens;
    }

    public long getTotalTokens() {
        return inputTokens + outputTokens;
    }

    @Override
    public String toString() {
        return "UsageMetrics{" +
                "inputTokens=" + inputTokens +
                ", outputTokens=" + outputTokens +
                ", reasoningTokens=" + reasoningTokens +
                ", cacheCreationInputTokens=" + cacheCreationInputTokens +
                ", cacheReadInputTokens=" + cacheReadInputTokens +
                '}';
    }
}
