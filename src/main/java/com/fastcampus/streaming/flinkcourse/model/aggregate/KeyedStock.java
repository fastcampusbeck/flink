package com.fastcampus.streaming.flinkcourse.model.aggregate;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;

public class KeyedStock {
    private String key;
    private Stock stock;

    public KeyedStock() {
    }

    public KeyedStock(String key, Stock stock) {
        this.key = key;
        this.stock = stock;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Stock getStock() {
        return stock;
    }

    public void setStock(Stock stock) {
        this.stock = stock;
    }

    @Override
    public String toString() {
        return "KeyedStock{" +
                "key='" + key + '\'' +
                ", stock='" + stock + '\'' +
                '}';
    }
}
