package com.fastcampus.streaming.flinkcourse.model.stock;

import java.time.ZonedDateTime;

public class StockWithZonedDatetime extends Stock {
    private ZonedDateTime zonedDateTime;

    public StockWithZonedDatetime(String symbol, double price, long timestamp, ZonedDateTime zonedDateTime) {
        super(symbol, price, timestamp);
        this.zonedDateTime = zonedDateTime;
    }

    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    public void setZonedDateTime(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
    }

    @Override
    public String toString() {
        return "StockWithDatetime{" +
                "symbol='" + getSymbol() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                ", zonedDateTime=" + zonedDateTime +
                '}';
    }
}
