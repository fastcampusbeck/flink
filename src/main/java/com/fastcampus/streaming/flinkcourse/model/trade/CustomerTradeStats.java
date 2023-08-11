package com.fastcampus.streaming.flinkcourse.model.trade;

public class CustomerTradeStats {
    private double netProfit;
    private long tradeCount;

    public CustomerTradeStats() {
    }

    public CustomerTradeStats(double netProfit, long tradeCount) {
        this.netProfit = netProfit;
        this.tradeCount = tradeCount;
    }

    public double getNetProfit() {
        return netProfit;
    }

    public void setNetProfit(double netProfit) {
        this.netProfit = netProfit;
    }

    public long getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(long tradeCount) {
        this.tradeCount = tradeCount;
    }

    @Override
    public String toString() {
        return "CustomerTradeStats{" +
                "netProfit=" + netProfit +
                ", tradeCount=" + tradeCount +
                '}';
    }

}
