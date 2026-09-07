package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Tool definition for dynamic registration via JSON-RPC.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RegisterToolDefinition {
    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("parameters")
    private JsonNode parameters;

    @JsonProperty("supports_parallel")
    private boolean supportsParallel = true;

    @JsonProperty("require_approval")
    private boolean requireApproval = false;

    @JsonProperty("is_host_tool")
    private boolean isHostTool = true;

    public RegisterToolDefinition() {
    }

    public RegisterToolDefinition(String name, String description, JsonNode parameters,
                                  boolean supportsParallel, boolean requireApproval, boolean isHostTool) {
        this.name = name;
        this.description = description;
        this.parameters = parameters;
        this.supportsParallel = supportsParallel;
        this.requireApproval = requireApproval;
        this.isHostTool = isHostTool;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public JsonNode getParameters() {
        return parameters;
    }

    public void setParameters(JsonNode parameters) {
        this.parameters = parameters;
    }

    public boolean isSupportsParallel() {
        return supportsParallel;
    }

    public void setSupportsParallel(boolean supportsParallel) {
        this.supportsParallel = supportsParallel;
    }

    public boolean isRequireApproval() {
        return requireApproval;
    }

    public void setRequireApproval(boolean requireApproval) {
        this.requireApproval = requireApproval;
    }

    public boolean isHostTool() {
        return isHostTool;
    }

    public void setHostTool(boolean hostTool) {
        isHostTool = hostTool;
    }
}
