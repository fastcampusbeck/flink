package com.fastcampus.streaming.flinkcourse.model.stock;

public class SectorStock extends Stock {
    private String sector;

    public SectorStock() {
    }

    public SectorStock(String symbol, double price, long timestamp, String sector) {
        super(symbol, price, timestamp);
        this.sector = sector;
    }

    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    @Override
    public String toString() {
        return "SectorStock{" +
                "sector='" + sector + '\'' +
                ", symbol='" + getSymbol() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
