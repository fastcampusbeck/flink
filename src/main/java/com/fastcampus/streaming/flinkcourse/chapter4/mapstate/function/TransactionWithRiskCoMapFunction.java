package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithRisk;
import com.fastcampus.streaming.flinkcourse.model.asset.RiskUpdate;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.HashMap;
import java.util.Map;

public class TransactionWithRiskCoMapFunction implements CoMapFunction<AssetTradeWithCustomer, RiskUpdate, AssetTradeWithRisk> {
    private static final Map<String, Double> RISK_WEIGHTS = new HashMap<>();

    @Override
    public AssetTradeWithRisk map1(AssetTradeWithCustomer assetTradeWithCustomer) throws Exception {
        double riskWeight = RISK_WEIGHTS.getOrDefault(assetTradeWithCustomer.getAsset(), 1.0);
        return new AssetTradeWithRisk(assetTradeWithCustomer.getAsset(), assetTradeWithCustomer.getIsBuy(),
                assetTradeWithCustomer.getPrice(), assetTradeWithCustomer.getAmount(),
                assetTradeWithCustomer.getCustomerId(), riskWeight);
    }

    @Override
    public AssetTradeWithRisk map2(RiskUpdate riskUpdate) throws Exception {
        RISK_WEIGHTS.put(riskUpdate.getAsset(), riskUpdate.getRiskWeight());
        return null;
    }
}
