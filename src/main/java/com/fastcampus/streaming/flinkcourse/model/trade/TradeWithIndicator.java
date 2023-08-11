package com.fastcampus.streaming.flinkcourse.model.trade;

public class TradeWithIndicator extends Trade {
    private String indicator;

    public TradeWithIndicator() {
    }

    public TradeWithIndicator(String exchange, String securityId, long timestamp, double tradePrice, double tradeVolume, String indicator) {
        super(exchange, securityId, timestamp, tradePrice, tradeVolume);
        this.indicator = indicator;
    }

    public String getIndicator() {
        return indicator;
    }

    public void setIndicator(String indicator) {
        this.indicator = indicator;
    }

    @Override
    public String toString() {
        return "TradeWithIndicator{" +
                "exchange='" + getExchange() + '\'' +
                ", securityId='" + getSecurityId() + '\'' +
                ", timestamp=" + getTimestamp() +
                ", tradePrice=" + getTradePrice() +
                ", tradeVolume=" + getTradeVolume() +
                ", indicator='" + indicator + '\'' +
                '}';
    }
}
