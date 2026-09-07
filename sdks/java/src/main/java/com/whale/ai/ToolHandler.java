package com.whale.ai;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Functional interface for executing tools on the Java host side.
 */
@FunctionalInterface
public interface ToolHandler {
    /**
     * Executes the tool with the given JSON arguments.
     *
     * @param arguments input arguments passed by LLM
     * @return result as a JsonNode
     * @throws Exception if execution fails
     */
    JsonNode execute(JsonNode arguments) throws Exception;
}
