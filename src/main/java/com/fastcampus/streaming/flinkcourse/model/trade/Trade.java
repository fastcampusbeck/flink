package com.fastcampus.streaming.flinkcourse.model.trade;

public class Trade {

    private String exchange;
    private String securityId;
    private long timestamp;
    private double tradePrice;
    private double tradeVolume;

    public Trade() {
    }

    public Trade(String exchange, String securityId, long timestamp, double tradePrice, double tradeVolume) {
        this.exchange = exchange;
        this.securityId = securityId;
        this.timestamp = timestamp;
        this.tradePrice = tradePrice;
        this.tradeVolume = tradeVolume;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(String securityId) {
        this.securityId = securityId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTradePrice() {
        return tradePrice;
    }

    public void setTradePrice(double tradePrice) {
        this.tradePrice = tradePrice;
    }

    public double getTradeVolume() {
        return tradeVolume;
    }

    public void setTradeVolume(double tradeVolume) {
        this.tradeVolume = tradeVolume;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "exchange='" + exchange + '\'' +
                ", securityId='" + securityId + '\'' +
                ", timestamp=" + timestamp +
                ", tradePrice=" + tradePrice +
                ", tradeVolume=" + tradeVolume +
                '}';
    }
}
