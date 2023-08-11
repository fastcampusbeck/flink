package com.fastcampus.streaming.flinkcourse.model.user;

public class LoginEvent extends UserEvent {
    public LoginEvent() {
    }

    public LoginEvent(String userId, String sessionId, long timestamp) {
        super(userId, sessionId, timestamp);
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + getUserId() + '\'' +
                ", sessionId='" + getSessionId() + '\'' +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
