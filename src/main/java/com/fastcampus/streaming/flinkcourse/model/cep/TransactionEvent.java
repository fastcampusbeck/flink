package com.fastcampus.streaming.flinkcourse.model.cep;

import java.util.Objects;

public class TransactionEvent {
    private String userId;
    private double amount;

    public TransactionEvent() {
    }

    public TransactionEvent(String userId, double amount) {
        this.userId = userId;
        this.amount = amount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionEvent that = (TransactionEvent) o;
        return Double.compare(that.amount, amount) == 0 && Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, amount);
    }
}
