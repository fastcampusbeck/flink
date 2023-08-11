package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTrade;
import com.fastcampus.streaming.flinkcourse.model.asset.PortfolioMetrics;
import com.fastcampus.streaming.flinkcourse.model.asset.PriceUpdate;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PortfolioMetricsAggregateFunction implements AggregateFunction<Tuple2<AssetTrade, PriceUpdate>, PortfolioMetrics, PortfolioMetrics> {
    @Override
    public PortfolioMetrics createAccumulator() {
        return new PortfolioMetrics();
    }

    @Override
    public PortfolioMetrics add(Tuple2<AssetTrade, PriceUpdate> assetTradePriceUpdateTuple2, PortfolioMetrics portfolioMetrics) {
        AssetTrade assetTrade = assetTradePriceUpdateTuple2.f0;
        PriceUpdate priceUpdate = assetTradePriceUpdateTuple2.f1;

        if(assetTrade != null) {
            double transactionValue = assetTrade.getPrice() * assetTrade.getAmount();

            if (assetTrade.getIsBuy()) {
                portfolioMetrics.setTotalCost(portfolioMetrics.getTotalCost() + transactionValue);
                portfolioMetrics.getAssetAmounts()
                        .compute(assetTrade.getAsset(),
                                (asset, amount) -> amount == null ? assetTrade.getAmount() : amount + assetTrade.getAmount());
            } else {
                portfolioMetrics.getAssetAmounts().computeIfPresent(assetTrade.getAsset(), (asset, amount) -> {
                    int amountAfterSell = amount - assetTrade.getAmount();
                    return Math.max(amountAfterSell, 0);
                });
            }
            portfolioMetrics.getLatestPrices().put(assetTrade.getAsset(), assetTrade.getPrice());
        }

        if (priceUpdate != null) {
            double previousLatestPrice = portfolioMetrics.getLatestPrices().getOrDefault(priceUpdate.getAsset(), Double.MIN_VALUE);
            if (previousLatestPrice != Double.MIN_VALUE) {
                double priceAfterUpdate = previousLatestPrice + priceUpdate.getPrice();
                portfolioMetrics.getLatestPrices().put(priceUpdate.getAsset(), Math.max(priceAfterUpdate, 0.0));
            }
        }

        portfolioMetrics.setTotalValue(
                portfolioMetrics.getAssetAmounts().entrySet().stream()
                        .filter(entry -> portfolioMetrics.getLatestPrices().containsKey(entry.getKey()))
                        .mapToDouble(entry -> entry.getValue() * portfolioMetrics.getLatestPrices().get(entry.getKey()))
                        .sum()
        );

        return portfolioMetrics;
    }

    @Override
    public PortfolioMetrics getResult(PortfolioMetrics portfolioMetrics) {
        return portfolioMetrics;
    }

    @Override
    public PortfolioMetrics merge(PortfolioMetrics portfolioMetrics, PortfolioMetrics acc1) {
        acc1.getAssetAmounts().forEach((asset, amount) -> portfolioMetrics.getAssetAmounts().merge(asset, amount, Integer::sum));
        portfolioMetrics.setTotalValue(portfolioMetrics.getTotalValue() + acc1.getTotalValue());
        portfolioMetrics.setTotalCost(portfolioMetrics.getTotalCost() + acc1.getTotalCost());
        acc1.getLatestPrices().forEach(portfolioMetrics.getLatestPrices()::put);
        return portfolioMetrics;
    }
}
