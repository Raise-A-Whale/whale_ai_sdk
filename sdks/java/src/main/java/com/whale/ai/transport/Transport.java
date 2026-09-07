package com.whale.ai.transport;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Abstract transport interface for JSON-RPC communication.
 */
public interface Transport extends Closeable {
    /**
     * Sends a JSON payload to the remote peer.
     */
    void send(JsonNode message) throws IOException;

    /**
     * Sets the consumer for incoming JSON messages.
     */
    void setMessageHandler(Consumer<JsonNode> handler);

    /**
     * Checks if the transport is active and connected.
     */
    boolean isAlive();

    /**
     * Closes the transport.
     */
    @Override
    void close() throws IOException;
}
