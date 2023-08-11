package com.fastcampus.streaming.flinkcourse.model.recommendation;

public class BotDirective {
    private String botId;
    private String directiveType; // BUY, SELL, or HOLD
    private String symbol;
    private double minPrice;
    private double maxPrice;
    private long timestamp;
    private long expirationTimestamp;

    public BotDirective() {
    }

    public BotDirective(String botId, String directiveType, String symbol, double minPrice, double maxPrice, long timestamp, long expirationTimestamp) {
        this.botId = botId;
        this.directiveType = directiveType;
        this.symbol = symbol;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
        this.timestamp = timestamp;
        this.expirationTimestamp = expirationTimestamp;
    }

    public String getBotId() {
        return botId;
    }

    public void setBotId(String botId) {
        this.botId = botId;
    }

    public String getDirectiveType() {
        return directiveType;
    }

    public void setDirectiveType(String directiveType) {
        this.directiveType = directiveType;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getMinPrice() {
        return minPrice;
    }

    public void setMinPrice(double minPrice) {
        this.minPrice = minPrice;
    }

    public double getMaxPrice() {
        return maxPrice;
    }

    public void setMaxPrice(double maxPrice) {
        this.maxPrice = maxPrice;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getExpirationTimestamp() {
        return expirationTimestamp;
    }

    public void setExpirationTimestamp(long expirationTimestamp) {
        this.expirationTimestamp = expirationTimestamp;
    }

    @Override
    public String toString() {
        return "BotDirective{" +
                "botId='" + botId + '\'' +
                ", directiveType='" + directiveType + '\'' +
                ", symbol='" + symbol + '\'' +
                ", minPrice=" + minPrice +
                ", maxPrice=" + maxPrice +
                ", timestamp=" + timestamp +
                ", expirationTimestamp=" + expirationTimestamp +
                '}';
    }
}
