package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Canonical representation of tool output payload.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CanonicalToolOutput {
    @JsonProperty("type")
    private String type; // "text", "structured", or "blocks"

    @JsonProperty("text")
    private String text;

    @JsonProperty("data")
    private JsonNode data;

    @JsonProperty("blocks")
    private List<CanonicalContent> blocks;

    public CanonicalToolOutput() {
    }

    public static CanonicalToolOutput fromText(String text) {
        CanonicalToolOutput output = new CanonicalToolOutput();
        output.setType("text");
        output.setText(text);
        return output;
    }

    public static CanonicalToolOutput fromStructured(JsonNode data) {
        CanonicalToolOutput output = new CanonicalToolOutput();
        output.setType("structured");
        output.setData(data);
        return output;
    }

    public static CanonicalToolOutput fromBlocks(List<CanonicalContent> blocks) {
        CanonicalToolOutput output = new CanonicalToolOutput();
        output.setType("blocks");
        output.setBlocks(blocks);
        return output;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }

    public List<CanonicalContent> getBlocks() {
        return blocks;
    }

    public void setBlocks(List<CanonicalContent> blocks) {
        this.blocks = blocks;
    }

    @Override
    public String toString() {
        return "CanonicalToolOutput{" +
                "type='" + type + '\'' +
                ", text='" + text + '\'' +
                ", data=" + data +
                ", blocks=" + blocks +
                '}';
    }
}
