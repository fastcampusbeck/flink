package com.fastcampus.streaming.flinkcourse.model.user;

public class ViewEvent extends UserEvent {
    private String stockId;

    public ViewEvent() {
    }

    public ViewEvent(String userId, String sessionId, long timestamp, String stockId) {
        super(userId, sessionId, timestamp);
        this.stockId = stockId;
    }

    public String getStockId() {
        return stockId;
    }

    public void setStockId(String stockId) {
        this.stockId = stockId;
    }

    @Override
    public String toString() {
        return "ViewEvent{" +
                "userId='" + getUserId() + '\'' +
                ", sessionId='" + getSessionId() + '\'' +
                ", timestamp=" + getTimestamp() +
                ", stockId='" + stockId + '\'' +
                '}';
    }
}
