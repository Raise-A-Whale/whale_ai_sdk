package com.whale.ai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.whale.ai.models.AgentStreamEvent;
import com.whale.ai.models.ApprovalDecision;
import com.whale.ai.models.CanonicalToolOutput;
import com.whale.ai.transport.ProcessStdioTransport;
import com.whale.ai.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Whale AI SDK Client managing connection, Reverse RPC routing, and thread dispatch.
 */
public class WhaleClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(WhaleClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Transport transport;
    private final Map<String, CompletableFuture<JsonNode>> pendingRequests = new ConcurrentHashMap<>();
    private final Map<String, Consumer<AgentStreamEvent>> eventConsumers = new ConcurrentHashMap<>();
    private final Map<String, Tool> hostTools = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "whale-client-worker");
        t.setDaemon(true);
        return t;
    });

    public WhaleClient() throws IOException {
        this(null, null, "info");
    }

    public WhaleClient(String customDaemonPath, List<String> extraArgs, String logLevel) throws IOException {
        this(new ProcessStdioTransport(customDaemonPath, extraArgs, logLevel));
    }

    public WhaleClient(Transport transport) {
        this.transport = transport;
        this.transport.setMessageHandler(this::handleIncomingMessage);
    }

    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    public Transport getTransport() {
        return transport;
    }

    /**
     * Dispatches an incoming JSON-RPC 2.0 message from whale-daemon.
     */
    void handleIncomingMessage(JsonNode msg) {
        logger.debug("Client received message: {}", msg);

        // 1. Response to a client request (has "id" and ("result" or "error"))
        if (msg.hasNonNull("id") && (msg.has("result") || msg.has("error"))) {
            String reqId = msg.get("id").asText();
            CompletableFuture<JsonNode> future = pendingRequests.remove(reqId);
            if (future != null) {
                if (msg.hasNonNull("error")) {
                    JsonNode errNode = msg.get("error");
                    long code = errNode.path("code").asLong(-32603);
                    String message = errNode.path("message").asText("Internal RPC error");
                    JsonNode data = errNode.get("data");
                    future.completeExceptionally(new RpcException(code, message, data));
                } else {
                    future.complete(msg.get("result"));
                }
            }
            return;
        }

        // 2. Notification from daemon (has "method" and no "id")
        if (msg.hasNonNull("method") && (!msg.has("id") || msg.get("id").isNull())) {
            String method = msg.get("method").asText();
            JsonNode params = msg.path("params");
            if ("turn.stream_events".equals(method)) {
                String threadId = params.path("thread_id").asText();
                JsonNode eventNode = params.get("event");
                if (threadId != null && eventNode != null) {
                    try {
                        AgentStreamEvent event = MAPPER.treeToValue(eventNode, AgentStreamEvent.class);
                        Consumer<AgentStreamEvent> consumer = eventConsumers.get(threadId);
                        if (consumer != null) {
                            consumer.accept(event);
                        }
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse AgentStreamEvent: {}", eventNode, e);
                    }
                }
            }
            return;
        }

        // 3. Reverse RPC Request from daemon to client (has "method" and "id")
        if (msg.hasNonNull("method") && msg.hasNonNull("id")) {
            String method = msg.get("method").asText();
            JsonNode idNode = msg.get("id");
            JsonNode params = msg.path("params");

            if ("tool.execute_host".equals(method)) {
                executor.submit(() -> executeHostToolAndRespond(idNode, params));
            } else {
                // Unknown method error
                ObjectNode errResp = MAPPER.createObjectNode();
                errResp.put("jsonrpc", "2.0");
                errResp.set("id", idNode);
                ObjectNode errorObj = MAPPER.createObjectNode();
                errorObj.put("code", -32601);
                errorObj.put("message", "Method not found: " + method);
                errResp.set("error", errorObj);
                try {
                    transport.send(errResp);
                } catch (IOException e) {
                    logger.error("Failed to send method_not_found response: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Executes host-side registered Tool and returns JSON-RPC response back to daemon.
     */
    private void executeHostToolAndRespond(JsonNode reqId, JsonNode params) {
        String callId = params.path("call_id").asText(UUID.randomUUID().toString());
        String toolName = params.path("name").asText();
        JsonNode arguments = params.path("arguments");

        Tool tool = hostTools.get(toolName);
        CanonicalToolOutput output;
        boolean isError = false;

        if (tool == null) {
            output = CanonicalToolOutput.fromText("Host tool '" + toolName + "' not registered in Java client");
            isError = true;
        } else {
            try {
                output = tool.execute(arguments);
            } catch (Exception e) {
                logger.error("Error executing host tool '{}': {}", toolName, e.getMessage(), e);
                output = CanonicalToolOutput.fromText("Tool '" + toolName + "' execution failed: " + e.getMessage());
                isError = true;
            }
        }

        ObjectNode response = MAPPER.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", reqId);

        ObjectNode resultObj = MAPPER.createObjectNode();
        resultObj.put("call_id", callId);
        resultObj.set("output", MAPPER.valueToTree(output));
        resultObj.put("is_error", isError);

        response.set("result", resultObj);

        try {
            transport.send(response);
        } catch (IOException e) {
            logger.error("Failed to send Reverse RPC response for call_id={}: {}", callId, e.getMessage());
        }
    }

    /**
     * Sends a JSON-RPC request and returns a future completing with the result.
     */
    public CompletableFuture<JsonNode> requestAsync(String method, JsonNode params) {
        String reqId = UUID.randomUUID().toString();
        CompletableFuture<JsonNode> future = new CompletableFuture<>();
        pendingRequests.put(reqId, future);

        ObjectNode req = MAPPER.createObjectNode();
        req.put("jsonrpc", "2.0");
        req.put("id", reqId);
        req.put("method", method);
        if (params != null) {
            req.set("params", params);
        }

        try {
            transport.send(req);
        } catch (IOException e) {
            pendingRequests.remove(reqId);
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Sends a synchronous JSON-RPC request with timeout.
     */
    public JsonNode request(String method, JsonNode params, long timeout, TimeUnit unit) {
        try {
            return requestAsync(method, params).get(timeout, unit);
        } catch (Exception e) {
            if (e.getCause() instanceof RpcException) {
                throw (RpcException) e.getCause();
            }
            throw new RuntimeException("RPC request failed for method '" + method + "': " + e.getMessage(), e);
        }
    }

    public JsonNode request(String method, JsonNode params) {
        return request(method, params, 60, TimeUnit.SECONDS);
    }

    /**
     * Registers a host tool in this client for reverse execution.
     */
    public Tool registerTool(Tool tool) {
        hostTools.put(tool.getName(), tool);
        return tool;
    }

    public Collection<Tool> getRegisteredTools() {
        return hostTools.values();
    }

    /**
     * Registers an event consumer for a thread.
     */
    void registerEventConsumer(String threadId, Consumer<AgentStreamEvent> consumer) {
        eventConsumers.put(threadId, consumer);
    }

    void unregisterEventConsumer(String threadId) {
        eventConsumers.remove(threadId);
    }

    /**
     * Starts a new conversation thread session.
     */
    public AgentThread createThread(String model, String systemPrompt, List<Tool> initialTools) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("model", model != null ? model : "claude-3-7-sonnet");
        if (systemPrompt != null) {
            params.put("system_prompt", systemPrompt);
        }

        ArrayNode toolsArray = MAPPER.createArrayNode();
        if (initialTools != null) {
            for (Tool t : initialTools) {
                registerTool(t);
                toolsArray.add(MAPPER.valueToTree(t.toDefinition()));
            }
        } else {
            for (Tool t : hostTools.values()) {
                toolsArray.add(MAPPER.valueToTree(t.toDefinition()));
            }
        }
        params.set("tools", toolsArray);

        JsonNode res = request("session.start_thread", params);
        String threadId = res.path("thread_id").asText();
        return new AgentThread(this, threadId);
    }

    public AgentThread createThread(String model, String systemPrompt) {
        return createThread(model, systemPrompt, null);
    }

    public AgentThread createThread(String model) {
        return createThread(model, null, null);
    }

    /**
     * Resolves a pending HITL tool approval request.
     */
    public boolean resolveApproval(String requestId, ApprovalDecision decision, String feedback) {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("request_id", requestId);
        params.put("decision", decision != null ? decision.getValue() : "approve");
        if (feedback != null) {
            params.put("feedback", feedback);
        }

        JsonNode res = request("approval.resolve", params);
        return res.path("resolved").asBoolean(false);
    }

    public boolean resolveApproval(String requestId, ApprovalDecision decision) {
        return resolveApproval(requestId, decision, null);
    }

    @Override
    public void close() {
        try {
            transport.close();
        } catch (IOException ignored) {
        }
        executor.shutdownNow();
    }
}
