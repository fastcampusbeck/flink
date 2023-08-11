package com.fastcampus.streaming.flinkcourse.model.trade;

public class EconomicIndicator {
    private String name;
    private long timestamp;
    private double value;

    public EconomicIndicator() {
    }

    public EconomicIndicator(String name, long timestamp, double value) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "EconomicIndicator{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
