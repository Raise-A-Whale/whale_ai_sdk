package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Canonical discrete item of a conversation turn.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CanonicalItem {
    @JsonProperty("type")
    private String type; // "user_message", "assistant_message", "reasoning", "tool_call", "tool_result"

    @JsonProperty("id")
    private String id;

    @JsonProperty("content")
    private List<CanonicalContent> content;

    @JsonProperty("phase")
    private MessagePhase phase;

    @JsonProperty("thinking")
    private String thinking;

    @JsonProperty("signature")
    private String signature;

    @JsonProperty("encrypted_content")
    private String encryptedContent;

    @JsonProperty("call_id")
    private String callId;

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("name")
    private String name;

    @JsonProperty("arguments")
    private JsonNode arguments;

    @JsonProperty("raw_arguments")
    private String rawArguments;

    @JsonProperty("output")
    private CanonicalToolOutput output;

    @JsonProperty("is_error")
    private Boolean isError;

    public CanonicalItem() {
        this.id = UUID.randomUUID().toString();
    }

    public static CanonicalItem userText(String text) {
        CanonicalItem item = new CanonicalItem();
        item.setType("user_message");
        item.setContent(Collections.singletonList(CanonicalContent.text(text)));
        return item;
    }

    public static CanonicalItem assistantText(String text, MessagePhase phase) {
        CanonicalItem item = new CanonicalItem();
        item.setType("assistant_message");
        item.setContent(Collections.singletonList(CanonicalContent.text(text)));
        item.setPhase(phase != null ? phase : MessagePhase.FINAL_ANSWER);
        return item;
    }

    public static CanonicalItem reasoning(String thinking, String signature) {
        CanonicalItem item = new CanonicalItem();
        item.setType("reasoning");
        item.setThinking(thinking);
        item.setSignature(signature);
        return item;
    }

    public static CanonicalItem toolCall(String callId, String name, JsonNode arguments, String rawArguments, String namespace) {
        CanonicalItem item = new CanonicalItem();
        item.setType("tool_call");
        item.setCallId(callId);
        item.setName(name);
        item.setArguments(arguments);
        item.setRawArguments(rawArguments);
        item.setNamespace(namespace);
        return item;
    }

    public static CanonicalItem toolResult(String callId, CanonicalToolOutput output, boolean isError) {
        CanonicalItem item = new CanonicalItem();
        item.setType("tool_result");
        item.setCallId(callId);
        item.setOutput(output);
        item.setIsError(isError);
        return item;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<CanonicalContent> getContent() {
        return content;
    }

    public void setContent(List<CanonicalContent> content) {
        this.content = content;
    }

    public MessagePhase getPhase() {
        return phase;
    }

    public void setPhase(MessagePhase phase) {
        this.phase = phase;
    }

    public String getThinking() {
        return thinking;
    }

    public void setThinking(String thinking) {
        this.thinking = thinking;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getEncryptedContent() {
        return encryptedContent;
    }

    public void setEncryptedContent(String encryptedContent) {
        this.encryptedContent = encryptedContent;
    }

    public String getCallId() {
        return callId;
    }

    public void setCallId(String callId) {
        this.callId = callId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JsonNode getArguments() {
        return arguments;
    }

    public void setArguments(JsonNode arguments) {
        this.arguments = arguments;
    }

    public String getRawArguments() {
        return rawArguments;
    }

    public void setRawArguments(String rawArguments) {
        this.rawArguments = rawArguments;
    }

    public CanonicalToolOutput getOutput() {
        return output;
    }

    public void setOutput(CanonicalToolOutput output) {
        this.output = output;
    }

    public Boolean getIsError() {
        return isError;
    }

    public void setIsError(Boolean isError) {
        this.isError = isError;
    }

    @Override
    public String toString() {
        return "CanonicalItem{" +
                "type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", callId='" + callId + '\'' +
                '}';
    }
}
