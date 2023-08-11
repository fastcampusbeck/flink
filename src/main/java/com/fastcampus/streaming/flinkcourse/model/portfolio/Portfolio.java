package com.fastcampus.streaming.flinkcourse.model.portfolio;

import java.util.List;

public class Portfolio {
    private String id;
    private List<Investment> investments;
    private long timestamp;
    private long validityPeriod;

    public Portfolio() {
    }

    public Portfolio(String id, List<Investment> investments, long timestamp, long validityPeriod) {
        this.id = id;
        this.investments = investments;
        this.timestamp = timestamp;
        this.validityPeriod = validityPeriod;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Investment> getInvestments() {
        return investments;
    }

    public void setInvestments(List<Investment> investments) {
        this.investments = investments;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getValidityPeriod() {
        return validityPeriod;
    }

    public void setValidityPeriod(long validityPeriod) {
        this.validityPeriod = validityPeriod;
    }

    public boolean isValid(long currentTime) {
        return (currentTime - timestamp) <= validityPeriod;
    }

    @Override
    public String toString() {
        return "Portfolio{" +
                "id='" + id + '\'' +
                ", investments=" + investments +
                ", timestamp=" + timestamp +
                ", validityPeriod=" + validityPeriod +
                '}';
    }
}
