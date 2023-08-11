package com.fastcampus.streaming.flinkcourse.model.news;

import java.util.Map;

public class StockNews {
    private String symbol;
    private long timestamp;
    private Map<String, String> updateInfo;

    public StockNews() {
    }

    public StockNews(String symbol, long timestamp, String importance, Map<String, String> updateInfo) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.updateInfo = updateInfo;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getUpdateInfo() {
        return updateInfo;
    }

    public void setUpdateInfo(Map<String, String> updateInfo) {
        this.updateInfo = updateInfo;
    }

    @Override
    public String toString() {
        return "StockNews{" +
                "symbol='" + symbol + '\'' +
                ", timestamp=" + timestamp +
                ", updateInfo='" + updateInfo + '\'' +
                '}';
    }

}
