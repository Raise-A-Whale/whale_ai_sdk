package com.whale.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.whale.ai.models.AgentStreamEvent;
import com.whale.ai.models.ApprovalDecision;
import com.whale.ai.models.CanonicalItem;
import com.whale.ai.models.CanonicalToolOutput;
import com.whale.ai.models.RunTurnResult;
import com.whale.ai.models.TurnStatus;
import com.whale.ai.transport.MockTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for JSON-RPC serialization, thread orchestration, and mock reverse tool execution.
 */
public class WhaleClientTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MockTransport transport;
    private WhaleClient client;

    @BeforeEach
    public void setUp() {
        transport = new MockTransport();
        client = new WhaleClient(transport);
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Test
    public void testToolBuilderAndExecution() throws Exception {
        Tool tool = Tool.builder()
                .name("calculate_square")
                .description("Calculates square of a number")
                .parameters("{\n" +
                        "  \"type\": \"object\",\n" +
                        "  \"properties\": {\n" +
                        "    \"n\": {\"type\": \"number\"}\n" +
                        "  },\n" +
                        "  \"required\": [\"n\"]\n" +
                        "}")
                .handler(args -> {
                    double n = args.path("n").asDouble();
                    ObjectNode res = MAPPER.createObjectNode();
                    res.put("result", n * n);
                    return res;
                })
                .build();

        assertEquals("calculate_square", tool.getName());
        assertEquals("Calculates square of a number", tool.getDescription());
        assertTrue(tool.isHostTool());
        assertTrue(tool.isSupportsParallel());

        ObjectNode callArgs = MAPPER.createObjectNode().put("n", 7.0);
        CanonicalToolOutput output = tool.execute(callArgs);
        assertEquals("structured", output.getType());
        assertEquals(49.0, output.getData().path("result").asDouble());
    }

    @Test
    public void testReverseRpcToolExecution() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        // Register host tool in client
        client.registerTool(Tool.builder()
                .name("add_numbers")
                .description("Adds two numbers")
                .parameters("{\n" +
                        "  \"type\": \"object\",\n" +
                        "  \"properties\": {\n" +
                        "    \"a\": {\"type\": \"number\"},\n" +
                        "    \"b\": {\"type\": \"number\"}\n" +
                        "  }\n" +
                        "}")
                .handler(args -> {
                    double a = args.path("a").asDouble();
                    double b = args.path("b").asDouble();
                    ObjectNode res = MAPPER.createObjectNode();
                    res.put("sum", a + b);
                    return res;
                })
                .build());

        transport.setOnSendHook(sentMsg -> {
            if (sentMsg.hasNonNull("id") && "reverse_call_001".equals(sentMsg.get("id").asText())) {
                latch.countDown();
            }
        });

        // Simulate incoming reverse RPC request from daemon
        String reverseRpcRequest = "{\n" +
                "  \"jsonrpc\": \"2.0\",\n" +
                "  \"id\": \"reverse_call_001\",\n" +
                "  \"method\": \"tool.execute_host\",\n" +
                "  \"params\": {\n" +
                "    \"call_id\": \"call_001\",\n" +
                "    \"name\": \"add_numbers\",\n" +
                "    \"arguments\": {\"a\": 12.5, \"b\": 27.5}\n" +
                "  }\n" +
                "}";

        transport.simulateIncoming(reverseRpcRequest);

        assertTrue(latch.await(3, TimeUnit.SECONDS), "Client did not respond to reverse RPC in time");

        List<JsonNode> sentMessages = transport.getSentMessages();
        JsonNode response = sentMessages.stream()
                .filter(m -> m.hasNonNull("id") && "reverse_call_001".equals(m.get("id").asText()))
                .findFirst()
                .orElse(null);

        assertNotNull(response);
        assertTrue(response.hasNonNull("result"));
        JsonNode resultNode = response.get("result");
        assertEquals("call_001", resultNode.path("call_id").asText());
        assertFalse(resultNode.path("is_error").asBoolean());
        assertEquals(40.0, resultNode.path("output").path("data").path("sum").asDouble());
    }

    @Test
    public void testThreadOrchestrationAndStreamingEvents() {
        transport.setOnSendHook(msg -> {
            String method = msg.path("method").asText();
            String id = msg.path("id").asText();

            if ("session.start_thread".equals(method)) {
                ObjectNode resp = MAPPER.createObjectNode();
                resp.put("jsonrpc", "2.0");
                resp.put("id", id);
                ObjectNode result = MAPPER.createObjectNode();
                result.put("thread_id", "thread_java_123");
                result.put("created_at", "2026-09-07T12:00:00Z");
                resp.set("result", result);
                transport.simulateIncoming(resp);
            } else if ("thread.run_turn".equals(method)) {
                String threadId = msg.path("params").path("thread_id").asText();

                // Stream reasoning delta event
                ObjectNode notif1 = MAPPER.createObjectNode();
                notif1.put("jsonrpc", "2.0");
                notif1.put("method", "turn.stream_events");
                ObjectNode p1 = MAPPER.createObjectNode();
                p1.put("thread_id", threadId);
                ObjectNode e1 = MAPPER.createObjectNode();
                e1.put("type", "reasoning_delta");
                e1.put("delta", "Analyzing repository files...");
                p1.set("event", e1);
                notif1.set("params", p1);
                transport.simulateIncoming(notif1);

                // Stream text delta event
                ObjectNode notif2 = MAPPER.createObjectNode();
                notif2.put("jsonrpc", "2.0");
                notif2.put("method", "turn.stream_events");
                ObjectNode p2 = MAPPER.createObjectNode();
                p2.put("thread_id", threadId);
                ObjectNode e2 = MAPPER.createObjectNode();
                e2.put("type", "text_delta");
                e2.put("delta", "The analysis is complete.");
                p2.set("event", e2);
                notif2.set("params", p2);
                transport.simulateIncoming(notif2);

                // Stream turn completed event
                ObjectNode notif3 = MAPPER.createObjectNode();
                notif3.put("jsonrpc", "2.0");
                notif3.put("method", "turn.stream_events");
                ObjectNode p3 = MAPPER.createObjectNode();
                p3.put("thread_id", threadId);
                ObjectNode e3 = MAPPER.createObjectNode();
                e3.put("type", "turn_completed");
                ObjectNode usage = MAPPER.createObjectNode();
                usage.put("input_tokens", 25);
                usage.put("output_tokens", 15);
                e3.set("usage", usage);
                p3.set("event", e3);
                notif3.set("params", p3);
                transport.simulateIncoming(notif3);

                // Send run_turn final response
                ObjectNode resp = MAPPER.createObjectNode();
                resp.put("jsonrpc", "2.0");
                resp.put("id", id);
                ObjectNode result = MAPPER.createObjectNode();
                result.put("turn_id", "turn_001");
                result.put("thread_id", threadId);
                result.put("status", "completed");
                result.set("usage", usage);
                ArrayNode items = MAPPER.createArrayNode();
                ObjectNode item = MAPPER.createObjectNode();
                item.put("type", "assistant_message");
                item.put("id", "msg_resp_1");
                ArrayNode content = MAPPER.createArrayNode();
                ObjectNode textBlock = MAPPER.createObjectNode();
                textBlock.put("type", "text");
                textBlock.put("text", "The analysis is complete.");
                content.add(textBlock);
                item.set("content", content);
                items.add(item);
                result.set("items", items);
                resp.set("result", result);
                transport.simulateIncoming(resp);
            }
        });

        AgentThread thread = client.createThread("claude-3-7-sonnet", "System instruction");
        assertEquals("thread_java_123", thread.getId());

        List<AgentStreamEvent> capturedEvents = Collections.synchronizedList(new ArrayList<>());
        RunTurnResult turnResult = thread.runTurn("Analyze project structure", capturedEvents::add);

        assertNotNull(turnResult);
        assertEquals("turn_001", turnResult.getTurnId());
        assertEquals(TurnStatus.COMPLETED, turnResult.getStatus());
        assertEquals(40, turnResult.getUsage().getTotalTokens());
        assertEquals(1, turnResult.getItems().size());
        assertEquals("assistant_message", turnResult.getItems().get(0).getType());

        assertEquals(3, capturedEvents.size());
        assertEquals("reasoning_delta", capturedEvents.get(0).getType());
        assertEquals("Analyzing repository files...", capturedEvents.get(0).getDelta());
        assertEquals("text_delta", capturedEvents.get(1).getType());
        assertEquals("The analysis is complete.", capturedEvents.get(1).getDelta());
        assertEquals("turn_completed", capturedEvents.get(2).getType());
    }

    @Test
    public void testApprovalResolution() {
        transport.setOnSendHook(msg -> {
            if ("approval.resolve".equals(msg.path("method").asText())) {
                String id = msg.path("id").asText();
                ObjectNode resp = MAPPER.createObjectNode();
                resp.put("jsonrpc", "2.0");
                resp.put("id", id);
                ObjectNode result = MAPPER.createObjectNode();
                result.put("resolved", true);
                result.put("request_id", msg.path("params").path("request_id").asText());
                resp.set("result", result);
                transport.simulateIncoming(resp);
            }
        });

        boolean approved = client.resolveApproval("req_456", ApprovalDecision.APPROVE, "Manual check passed");
        assertTrue(approved);

        List<JsonNode> sent = transport.getSentMessages();
        JsonNode resolveMsg = sent.stream()
                .filter(m -> "approval.resolve".equals(m.path("method").asText()))
                .findFirst()
                .orElse(null);

        assertNotNull(resolveMsg);
        assertEquals("req_456", resolveMsg.path("params").path("request_id").asText());
        assertEquals("approve", resolveMsg.path("params").path("decision").asText());
        assertEquals("Manual check passed", resolveMsg.path("params").path("feedback").asText());
    }
}
