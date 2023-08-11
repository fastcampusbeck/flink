package com.fastcampus.streaming.flinkcourse.model.commodity;

public class SilverCommodity extends Commodity {
    public SilverCommodity() {
    }

    public SilverCommodity(String exchange, double price, long timestamp) {
        this.exchange = exchange;
        this.commodity = "XAU";
        this.price = price;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SilverCommodity{" +
                "exchange='" + getExchange() + '\'' +
                ", commodity='" + getCommodity() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
