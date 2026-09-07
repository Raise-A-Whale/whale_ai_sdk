package com.whale.ai.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.whale.ai.AgentThread;
import com.whale.ai.Tool;
import com.whale.ai.WhaleClient;
import com.whale.ai.models.CanonicalItem;
import com.whale.ai.models.RunTurnResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * A runnable Java Agent example demonstrating:
 * 1. Defining a Java tool using Tool.builder(): "check_file_syntax"
 * 2. Running an Agent turn with Claude or GPT
 * 3. Handling stream events and Reverse RPC tool execution in pure Java!
 */
public class CodeReviewAgent {
    private static final Logger logger = LoggerFactory.getLogger(CodeReviewAgent.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        logger.info("Initializing Whale AI SDK Java Client...");

        try (WhaleClient client = new WhaleClient()) {
            // 1. Define a Java host tool using Tool.builder()
            Tool checkSyntaxTool = Tool.builder()
                    .name("check_file_syntax")
                    .description("Checks if a file exists on disk and validates basic syntax and line statistics")
                    .parameters("{\n" +
                            "  \"type\": \"object\",\n" +
                            "  \"properties\": {\n" +
                            "    \"file_path\": {\n" +
                            "      \"type\": \"string\",\n" +
                            "      \"description\": \"Absolute or relative path of the file to inspect\"\n" +
                            "    }\n" +
                            "  },\n" +
                            "  \"required\": [\"file_path\"]\n" +
                            "}")
                    .handler(argsNode -> {
                        String filePathStr = argsNode.path("file_path").asText();
                        logger.info("[Host Tool Invoked] check_file_syntax with path: {}", filePathStr);

                        Path path = Paths.get(filePathStr);
                        ObjectNode result = MAPPER.createObjectNode();

                        if (!Files.exists(path)) {
                            result.put("status", "error");
                            result.put("message", "File not found: " + filePathStr);
                            return result;
                        }

                        try {
                            long lineCount = Files.lines(path).count();
                            long byteSize = Files.size(path);
                            result.put("status", "valid");
                            result.put("file_name", path.getFileName().toString());
                            result.put("line_count", lineCount);
                            result.put("byte_size", byteSize);
                            result.put("readable", Files.isReadable(path));
                            return result;
                        } catch (Exception e) {
                            result.put("status", "error");
                            result.put("message", e.getMessage());
                            return result;
                        }
                    })
                    .build();

            // 2. Start a new agent thread session with Claude / GPT and register the tool
            logger.info("Creating Agent Thread with tool 'check_file_syntax'...");
            AgentThread thread = client.createThread(
                    "claude-3-7-sonnet",
                    "You are an expert code reviewer. When asked to inspect a file, you MUST use the `check_file_syntax` tool.",
                    Collections.singletonList(checkSyntaxTool)
            );

            logger.info("Thread created with id: {}", thread.getId());

            // 3. Execute an Agent turn with real-time event streaming
            String prompt = "Please check the syntax and line count of pom.xml in the current directory and provide a brief review.";
            System.out.println("\n>>> User: " + prompt + "\n");
            System.out.print(">>> Assistant: ");

            RunTurnResult result = thread.runTurn(prompt, event -> {
                switch (event.getType()) {
                    case "text_delta":
                        if (event.getDelta() != null) {
                            System.out.print(event.getDelta());
                            System.out.flush();
                        }
                        break;
                    case "reasoning_delta":
                        if (event.getDelta() != null) {
                            System.err.print(event.getDelta());
                            System.err.flush();
                        }
                        break;
                    case "item_completed":
                        if (event.getItem() != null && "tool_call".equals(event.getItem().getType())) {
                            System.out.println("\n[Agent requested tool call: " + event.getItem().getName() + "]");
                        }
                        break;
                    case "turn_completed":
                        System.out.println("\n[Turn completed successfully]");
                        break;
                    case "turn_failed":
                        System.err.println("\n[Turn failed: " + event.getErrorMessage() + "]");
                        break;
                    default:
                        break;
                }
            });

            System.out.println("\n\n--- Turn Summary ---");
            System.out.println("Status: " + result.getStatus());
            if (result.getUsage() != null) {
                System.out.println("Input tokens: " + result.getUsage().getInputTokens());
                System.out.println("Output tokens: " + result.getUsage().getOutputTokens());
                System.out.println("Total tokens: " + result.getUsage().getTotalTokens());
            }

        } catch (Exception e) {
            logger.error("Error executing CodeReviewAgent: {}", e.getMessage(), e);
        }
    }
}
