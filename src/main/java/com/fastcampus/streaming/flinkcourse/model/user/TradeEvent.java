package com.fastcampus.streaming.flinkcourse.model.user;

public class TradeEvent extends UserEvent {
    private String stockId;
    private double tradePrice;

    public TradeEvent() {
    }

    public TradeEvent(String userId, String sessionId, long timestamp, String stockId, double tradePrice) {
        super(userId, sessionId, timestamp);
        this.stockId = stockId;
        this.tradePrice = tradePrice;
    }

    public String getStockId() {
        return stockId;
    }

    public void setStockId(String stockId) {
        this.stockId = stockId;
    }

    public double getTradePrice() {
        return tradePrice;
    }

    public void setTradePrice(double tradePrice) {
        this.tradePrice = tradePrice;
    }

    @Override
    public String toString() {
        return "TradeEvent{" +
                "userId='" + getUserId() + '\'' +
                ", sessionId='" + getSessionId() + '\'' +
                ", timestamp=" + getTimestamp() +
                ", stockId='" + stockId + '\'' +
                ", tradePrice=" + tradePrice +
                '}';
    }
}
