package com.fastcampus.streaming.flinkcourse.model.aggregate;

public class StockPriceRange {
    private double min;
    private double max;

    public StockPriceRange() {
    }

    public StockPriceRange(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "StockPriceRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
