package com.fastcampus.streaming.flinkcourse.model.stock;

public class StockWithConverged extends Stock {
    private boolean converged;

    public StockWithConverged() {
    }

    public StockWithConverged(String symbol, double price, long timestamp, boolean converged) {
        super(symbol, price, timestamp);
        this.converged = converged;
    }

    public boolean getConverged() {
        return converged;
    }

    public void setConverged(boolean converged) {
        this.converged = converged;
    }

    @Override
    public String toString() {
        return "StockWithConverged{" +
                "symbol='" + getSymbol() + '\'' +
                ", price=" + getPrice() +
                ", timestamp=" + getTimestamp() +
                "converged=" + converged +
                '}';
    }
}
