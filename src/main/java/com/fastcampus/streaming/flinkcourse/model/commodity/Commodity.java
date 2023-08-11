package com.fastcampus.streaming.flinkcourse.model.commodity;

public class Commodity {
    protected String exchange;
    protected String commodity;
    protected double price;
    protected long timestamp;

    public Commodity() {
    }

    public Commodity(String exchange, String commodity, double price, long timestamp) {
        this.exchange = exchange;
        this.commodity = commodity;
        this.price = price;
        this.timestamp = timestamp;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Commodity{" +
                "exchange='" + exchange + '\'' +
                ", commodity='" + commodity + '\'' +
                ", price=" + price +
                ", timestamp=" + timestamp +
                '}';
    }
}
