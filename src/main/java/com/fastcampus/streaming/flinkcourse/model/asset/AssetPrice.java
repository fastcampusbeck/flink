package com.fastcampus.streaming.flinkcourse.model.asset;

public class AssetPrice {
    private String assetName;
    private double price;
    private String portfolioId;
    private long timestamp;

    public AssetPrice() {
    }

    public AssetPrice(String assetName, double price, String portfolioId, long timestamp) {
        this.assetName = assetName;
        this.price = price;
        this.portfolioId = portfolioId;
        this.timestamp = timestamp;
    }

    public String getAssetName() {
        return assetName;
    }

    public void setAssetName(String assetName) {
        this.assetName = assetName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AssetPrice{" +
                "assetName='" + assetName + '\'' +
                ", price=" + price +
                ", portfolioId='" + portfolioId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
