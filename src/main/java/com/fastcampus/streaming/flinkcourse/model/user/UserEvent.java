package com.fastcampus.streaming.flinkcourse.model.user;

public class UserEvent {
    private String userId;
    private String sessionId;
    private long timestamp;

    public UserEvent() {
    }

    public UserEvent(String userId, String sessionId, long timestamp) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "userId='" + userId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
