package com.fastcampus.streaming.flinkcourse.model.cep;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class WebAlert {
    private String message;
    private Map<String, List<WebEvent>> pattern;

    public WebAlert() {
    }

    public WebAlert(String message, Map<String, List<WebEvent>> pattern) {
        this.message = message;
        this.pattern = pattern;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, List<WebEvent>> getPattern() {
        return pattern;
    }

    public void setPattern(Map<String, List<WebEvent>> pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WebAlert webAlert = (WebAlert) o;
        return Objects.equals(message, webAlert.message) && Objects.equals(pattern, webAlert.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, pattern);
    }

    @Override
    public String toString() {
        return "WebAlert{" +
                "message='" + message + '\'' +
                ", pattern=" + pattern +
                '}';
    }
}
