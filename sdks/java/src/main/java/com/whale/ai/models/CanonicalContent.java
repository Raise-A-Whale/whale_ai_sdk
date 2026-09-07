package com.whale.ai.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Multi-modal content block supported in canonical messages.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CanonicalContent {
    @JsonProperty("type")
    private String type; // "text", "image", "audio"

    @JsonProperty("text")
    private String text;

    @JsonProperty("mime_type")
    private String mimeType;

    @JsonProperty("data")
    private String data;

    @JsonProperty("uri")
    private String uri;

    public CanonicalContent() {
    }

    public static CanonicalContent text(String text) {
        CanonicalContent content = new CanonicalContent();
        content.setType("text");
        content.setText(text);
        return content;
    }

    public static CanonicalContent imageUri(String mimeType, String uri) {
        CanonicalContent content = new CanonicalContent();
        content.setType("image");
        content.setMimeType(mimeType);
        content.setUri(uri);
        return content;
    }

    public static CanonicalContent imageBase64(String mimeType, String base64Data) {
        CanonicalContent content = new CanonicalContent();
        content.setType("image");
        content.setMimeType(mimeType);
        content.setData(base64Data);
        return content;
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

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public String toString() {
        return "CanonicalContent{" +
                "type='" + type + '\'' +
                ", text='" + text + '\'' +
                ", mimeType='" + mimeType + '\'' +
                ", uri='" + uri + '\'' +
                '}';
    }
}
