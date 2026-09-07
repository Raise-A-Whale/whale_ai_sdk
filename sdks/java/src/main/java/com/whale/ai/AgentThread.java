package com.whale.ai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.whale.ai.models.AgentStreamEvent;
import com.whale.ai.models.ApprovalDecision;
import com.whale.ai.models.CanonicalItem;
import com.whale.ai.models.RunTurnResult;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Represents an active conversation thread session with an Agent.
 */
public class AgentThread {
    private static final ObjectMapper MAPPER = WhaleClient.getMapper();

    private final WhaleClient client;
    private final String threadId;

    public AgentThread(WhaleClient client, String threadId) {
        this.client = client;
        this.threadId = threadId;
    }

    public String getId() {
        return threadId;
    }

    public WhaleClient getClient() {
        return client;
    }

    /**
     * Dynamically registers a tool with both the Java client and the daemon session.
     */
    public Tool registerTool(Tool tool) {
        client.registerTool(tool);

        ObjectNode params = MAPPER.createObjectNode();
        params.put("thread_id", threadId);

        ArrayNode toolsArray = MAPPER.createArrayNode();
        toolsArray.add(MAPPER.valueToTree(tool.toDefinition()));
        params.set("tools", toolsArray);

        client.request("session.register_tools", params);
        return tool;
    }

    /**
     * Runs an agent turn with a user text prompt and an optional streaming event consumer.
     */
    public RunTurnResult runTurn(String prompt, Consumer<AgentStreamEvent> onEvent, TurnOptions options, long timeout, TimeUnit unit) {
        return runTurn(Collections.singletonList(CanonicalItem.userText(prompt)), onEvent, options, timeout, unit);
    }

    public RunTurnResult runTurn(String prompt, Consumer<AgentStreamEvent> onEvent) {
        return runTurn(prompt, onEvent, null, 120, TimeUnit.SECONDS);
    }

    public RunTurnResult runTurn(String prompt) {
        return runTurn(prompt, null, null, 120, TimeUnit.SECONDS);
    }

    /**
     * Asynchronously runs an agent turn with input canonical items and an event consumer.
     */
    public CompletableFuture<RunTurnResult> runTurnAsync(List<CanonicalItem> items, Consumer<AgentStreamEvent> onEvent, TurnOptions options) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("thread_id", threadId);

        ArrayNode itemsArray = MAPPER.createArrayNode();
        for (CanonicalItem item : items) {
            itemsArray.add(MAPPER.valueToTree(item));
        }
        params.set("input_items", itemsArray);

        if (options != null) {
            params.set("options", MAPPER.valueToTree(options));
        }

        if (onEvent != null) {
            client.registerEventConsumer(threadId, onEvent);
        }

        return client.requestAsync("thread.run_turn", params)
                .thenApply(resNode -> {
                    try {
                        return MAPPER.treeToValue(resNode, RunTurnResult.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Failed to deserialize RunTurnResult: " + e.getMessage(), e);
                    }
                })
                .whenComplete((res, ex) -> {
                    if (onEvent != null) {
                        client.unregisterEventConsumer(threadId);
                    }
                });
    }

    /**
     * Synchronously runs an agent turn with input canonical items and an event consumer.
     */
    public RunTurnResult runTurn(List<CanonicalItem> items, Consumer<AgentStreamEvent> onEvent, TurnOptions options, long timeout, TimeUnit unit) {
        try {
            return runTurnAsync(items, onEvent, options).get(timeout, unit);
        } catch (Exception e) {
            if (e.getCause() instanceof RpcException) {
                throw (RpcException) e.getCause();
            }
            throw new RuntimeException("Turn execution failed: " + e.getMessage(), e);
        }
    }

    /**
     * Resolves a pending Human-in-the-loop approval decision.
     */
    public boolean resolveApproval(String requestId, ApprovalDecision decision, String feedback) {
        return client.resolveApproval(requestId, decision, feedback);
    }

    public boolean resolveApproval(String requestId, ApprovalDecision decision) {
        return client.resolveApproval(requestId, decision, null);
    }

    /**
     * Options for turn execution.
     */
    public static class TurnOptions {
        private String model;
        private Double temperature;
        private Integer maxTokens;
        private String reasoningEffort;

        public TurnOptions() {
        }

        public String getModel() {
            return model;
        }

        public TurnOptions setModel(String model) {
            this.model = model;
            return this;
        }

        public Double getTemperature() {
            return temperature;
        }

        public TurnOptions setTemperature(Double temperature) {
            this.temperature = temperature;
            return this;
        }

        public Integer getMaxTokens() {
            return maxTokens;
        }

        public TurnOptions setMaxTokens(Integer maxTokens) {
            this.maxTokens = maxTokens;
            return this;
        }

        public String getReasoningEffort() {
            return reasoningEffort;
        }

        public TurnOptions setReasoningEffort(String reasoningEffort) {
            this.reasoningEffort = reasoningEffort;
            return this;
        }
    }
}
