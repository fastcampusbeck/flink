package com.fastcampus.streaming.flinkcourse.model.user;

import java.util.ArrayList;
import java.util.List;

public class UserActivity {
    private String userId;
    private String sessionId;
    private List<String> activities;
    private long timestamp;

    public UserActivity() {
    }

    public UserActivity(String userId, String sessionId, String activity, long timestamp) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.activities = new ArrayList<>();
        this.activities.add(activity);
        this.timestamp = timestamp;
    }

    public void addActivity(String activity) {
        this.activities.add(activity);
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

    public List<String> getActivities() {
        return activities;
    }

    public void setActivities(List<String> activities) {
        this.activities = activities;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserActivity{" +
                "userId='" + userId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", activities=" + activities +
                ", timestamp=" + timestamp +
                '}';
    }
}
