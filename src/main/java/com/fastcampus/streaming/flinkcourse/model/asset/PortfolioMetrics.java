package com.fastcampus.streaming.flinkcourse.model.asset;

import java.util.HashMap;
import java.util.Map;

public class PortfolioMetrics {

    private Map<String, Integer> assetAmounts;
    private Map<String, Double> latestPrices;
    private double totalCost;
    private double totalValue;

    public PortfolioMetrics() {
        this.assetAmounts = new HashMap<>();
        this.latestPrices = new HashMap<>();
        this.totalCost = 0;
        this.totalValue = 0;
    }

    public PortfolioMetrics(Map<String, Integer> assetAmounts, Map<String, Double> latestPrices, double totalCost, double totalValue) {
        this.assetAmounts = assetAmounts;
        this.latestPrices = latestPrices;
        this.totalCost = totalCost;
        this.totalValue = totalValue;
    }

    public double getValueToCostRatio() {
        return totalCost != 0 ? totalValue / totalCost : 0;
    }

    public Map<String, Integer> getAssetAmounts() {
        return assetAmounts;
    }

    public void setAssetAmounts(Map<String, Integer> assetAmounts) {
        this.assetAmounts = assetAmounts;
    }

    public Map<String, Double> getLatestPrices() {
        return latestPrices;
    }

    public void setLatestPrices(Map<String, Double> latestPrices) {
        this.latestPrices = latestPrices;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(double totalCost) {
        this.totalCost = totalCost;
    }

    public double getTotalValue() {
        return totalValue;
    }

    public void setTotalValue(double totalValue) {
        this.totalValue = totalValue;
    }

    @Override
    public String toString() {
        return "PortfolioMetrics{" +
                "assetAmounts=" + assetAmounts +
                "latestPrices=" + latestPrices +
                ", totalCost=" + totalCost +
                ", totalValue=" + totalValue +
                '}';
    }
}
