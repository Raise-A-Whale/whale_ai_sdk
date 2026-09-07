package com.whale.ai;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Exception representing a JSON-RPC protocol or remote invocation error.
 */
public class RpcException extends RuntimeException {
    private final long code;
    private final JsonNode data;

    public RpcException(long code, String message, JsonNode data) {
        super(String.format("RPC Error [%d]: %s", code, message));
        this.code = code;
        this.data = data;
    }

    public long getCode() {
        return code;
    }

    public JsonNode getData() {
        return data;
    }
}
