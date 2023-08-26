package com.fastcampus.streaming.flinkcourse.model.cep;

import java.util.Objects;

public class WebEvent {
    private long timestamp;
    private String type;

    public WebEvent() {
    }

    public WebEvent(String type) {
        this.timestamp = System.currentTimeMillis();
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WebEvent webEvent = (WebEvent) o;
        return timestamp == webEvent.timestamp && Objects.equals(type, webEvent.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, type);
    }

    @Override
    public String toString() {
        return "WebEvent{" +
                "timestamp=" + timestamp +
                ", type='" + type + '\'' +
                '}';
    }
}
