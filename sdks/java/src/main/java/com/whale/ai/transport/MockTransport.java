package com.whale.ai.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * In-memory MockTransport for unit testing and deterministic simulation.
 */
public class MockTransport implements Transport {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<JsonNode> sentMessages = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private Consumer<JsonNode> messageHandler;
    private Consumer<JsonNode> onSendHook;

    public void setOnSendHook(Consumer<JsonNode> hook) {
        this.onSendHook = hook;
    }

    @Override
    public void send(JsonNode message) throws IOException {
        if (!alive.get()) {
            throw new IOException("MockTransport is closed");
        }
        sentMessages.add(message);
        if (onSendHook != null) {
            onSendHook.accept(message);
        }
    }

    public void simulateIncoming(JsonNode message) {
        if (messageHandler != null) {
            messageHandler.accept(message);
        }
    }

    public void simulateIncoming(String jsonString) {
        try {
            simulateIncoming(MAPPER.readTree(jsonString));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse mock incoming JSON: " + e.getMessage(), e);
        }
    }

    public List<JsonNode> getSentMessages() {
        return new ArrayList<>(sentMessages);
    }

    @Override
    public void setMessageHandler(Consumer<JsonNode> handler) {
        this.messageHandler = handler;
    }

    @Override
    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public void close() {
        alive.set(false);
    }
}
