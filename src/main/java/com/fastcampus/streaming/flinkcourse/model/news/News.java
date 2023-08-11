package com.fastcampus.streaming.flinkcourse.model.news;

public class News {

    private String symbol;
    private String title;
    private boolean isPositive;

    private long timestamp;

    public News() {
    }

    public News(String symbol, String title, boolean isPositive, long timestamp) {
        this.symbol = symbol;
        this.title = title;
        this.isPositive = isPositive;
        this.timestamp = timestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public boolean getIsPositive() {
        return isPositive;
    }

    public void setIsPositive(boolean positive) {
        isPositive = positive;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "News{" +
                "symbol='" + symbol + '\'' +
                ", title='" + title + '\'' +
                ", isPositive='" + isPositive + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
