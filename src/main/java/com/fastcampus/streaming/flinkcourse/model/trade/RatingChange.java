package com.fastcampus.streaming.flinkcourse.model.trade;

public class RatingChange {
    private String securityId;
    private String newRating;
    private long timestamp;

    public RatingChange() {
    }

    public RatingChange(String securityId, String newRating, long timestamp) {
        this.securityId = securityId;
        this.newRating = newRating;
        this.timestamp = timestamp;
    }

    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(String securityId) {
        this.securityId = securityId;
    }

    public String getNewRating() {
        return newRating;
    }

    public void setNewRating(String newRating) {
        this.newRating = newRating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "RateChangeEvent{" +
                "securityId='" + securityId + '\'' +
                ", newRating='" + newRating + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
