package com.fastcampus.streaming.flinkcourse.model.aggregate;

public class PriceCount {
    private double price;
    private long count;

    public PriceCount() {
    }

    public PriceCount(double price, long count) {
        this.price = price;
        this.count = count;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PriceCount{" +
                "price=" + price +
                ", count=" + count +
                '}';
    }
}
