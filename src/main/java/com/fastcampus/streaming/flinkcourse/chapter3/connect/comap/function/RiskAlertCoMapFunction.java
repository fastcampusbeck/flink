package com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.function;

import com.fastcampus.streaming.flinkcourse.model.prediction.RiskScore;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class RiskAlertCoMapFunction implements CoMapFunction<StockTransaction, RiskScore, String> {
    private static final double RISK_SCORE_THRESHOLD = 0.7;  // Set your own risk score threshold

    @Override
    public String map1(StockTransaction stockTransaction) throws Exception {
        return "";
    }

    @Override
    public String map2(RiskScore riskScore) throws Exception {
        if (riskScore.getRiskScore() > RISK_SCORE_THRESHOLD) {
            return "Alert: High-risk trade detected for stock " + riskScore.getSymbol();
        } else {
            return "";
        }
    }
}
