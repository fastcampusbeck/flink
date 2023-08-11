package com.fastcampus.streaming.flinkcourse.model.user;

public class AnalysisEvent extends UserEvent {
    private String analysisResult;

    public AnalysisEvent() {
    }

    public AnalysisEvent(String userId, String sessionId, long timestamp, String analysisResult) {
        super(userId, sessionId, timestamp);
        this.analysisResult = analysisResult;
    }

    public String getAnalysisResult() {
        return analysisResult;
    }

    public void setAnalysisResult(String analysisResult) {
        this.analysisResult = analysisResult;
    }

    @Override
    public String toString() {
        return "AnalysisEvent{" +
                "LoginEvent{" +
                "userId='" + getUserId() + '\'' +
                ", sessionId='" + getSessionId() + '\'' +
                ", timestamp=" + getTimestamp() +
                ", analysisResult='" + analysisResult + '\'' +
                '}';
    }
}
