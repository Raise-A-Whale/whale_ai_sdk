package com.whale.ai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.whale.ai.models.CanonicalToolOutput;
import com.whale.ai.models.RegisterToolDefinition;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a callable tool with JSON Schema metadata for LLM invocation.
 */
public class Tool {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String name;
    private final String description;
    private final JsonNode parameters;
    private final Function<JsonNode, JsonNode> handler;
    private final boolean supportsParallel;
    private final boolean requireApproval;
    private final boolean hostTool;

    private Tool(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name must not be null");
        this.description = builder.description != null ? builder.description : "";
        this.parameters = builder.parameters != null ? builder.parameters : MAPPER.createObjectNode().put("type", "object");
        this.handler = Objects.requireNonNull(builder.handler, "handler must not be null");
        this.supportsParallel = builder.supportsParallel;
        this.requireApproval = builder.requireApproval;
        this.hostTool = builder.hostTool;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public JsonNode getParameters() {
        return parameters;
    }

    public Function<JsonNode, JsonNode> getHandler() {
        return handler;
    }

    public boolean isSupportsParallel() {
        return supportsParallel;
    }

    public boolean isRequireApproval() {
        return requireApproval;
    }

    public boolean isHostTool() {
        return hostTool;
    }

    /**
     * Executes the tool handler and wraps the output into a CanonicalToolOutput.
     */
    public CanonicalToolOutput execute(JsonNode arguments) throws Exception {
        JsonNode result = handler.apply(arguments);
        if (result == null || result.isNull()) {
            ObjectNode defaultNode = MAPPER.createObjectNode();
            defaultNode.put("status", "success");
            return CanonicalToolOutput.fromStructured(defaultNode);
        }
        if (result.isTextual()) {
            return CanonicalToolOutput.fromText(result.asText());
        }
        return CanonicalToolOutput.fromStructured(result);
    }

    /**
     * Converts to RPC registration model.
     */
    public RegisterToolDefinition toDefinition() {
        return new RegisterToolDefinition(
                name,
                description,
                parameters,
                supportsParallel,
                requireApproval,
                hostTool
        );
    }

    public static class Builder {
        private String name;
        private String description;
        private JsonNode parameters;
        private Function<JsonNode, JsonNode> handler;
        private boolean supportsParallel = true;
        private boolean requireApproval = false;
        private boolean hostTool = true;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder parameters(JsonNode parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder parameters(String jsonSchema) {
            try {
                this.parameters = MAPPER.readTree(jsonSchema);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid JSON Schema string: " + e.getMessage(), e);
            }
            return this;
        }

        public Builder handler(ToolHandler toolHandler) {
            this.handler = args -> {
                try {
                    return toolHandler.execute(args);
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException("Tool execution failed: " + e.getMessage(), e);
                }
            };
            return this;
        }

        public Builder handlerFunction(Function<JsonNode, JsonNode> handler) {
            this.handler = handler;
            return this;
        }

        public Builder executionLambda(Function<JsonNode, JsonNode> lambda) {
            this.handler = lambda;
            return this;
        }

        public Builder supportsParallel(boolean supportsParallel) {
            this.supportsParallel = supportsParallel;
            return this;
        }

        public Builder requireApproval(boolean requireApproval) {
            this.requireApproval = requireApproval;
            return this;
        }

        public Builder hostTool(boolean hostTool) {
            this.hostTool = hostTool;
            return this;
        }

        public Tool build() {
            return new Tool(this);
        }
    }
}
