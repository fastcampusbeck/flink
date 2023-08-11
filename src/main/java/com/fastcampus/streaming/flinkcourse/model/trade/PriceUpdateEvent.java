package com.fastcampus.streaming.flinkcourse.model.trade;

public class PriceUpdateEvent {
    private String asset;
    private double price;

    public PriceUpdateEvent() {
    }

    public PriceUpdateEvent(String asset, double price) {
        this.asset = asset;
        this.price = price;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "PriceUpdateEvent{" +
                "asset='" + asset + '\'' +
                ", price=" + price +
                '}';
    }

}
