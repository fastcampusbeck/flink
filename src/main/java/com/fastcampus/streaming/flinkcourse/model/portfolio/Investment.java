package com.fastcampus.streaming.flinkcourse.model.portfolio;

public class Investment {
    private String id;
    private double value;

    public Investment() {
    }

    public Investment(String id, double value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Investment{" +
                "id='" + id + '\'' +
                ", value=" + value +
                '}';
    }
}
