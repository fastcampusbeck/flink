package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTrade;
import com.fastcampus.streaming.flinkcourse.model.asset.PortfolioMetrics;
import com.fastcampus.streaming.flinkcourse.model.asset.PriceUpdate;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class ValueToCostRatioCoProcessFunction extends CoProcessFunction<AssetTrade, PriceUpdate, Tuple2<String, Double>> {
    private transient AggregatingState<Tuple2<AssetTrade, PriceUpdate>, PortfolioMetrics> portfolioState;


    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Tuple2<AssetTrade, PriceUpdate>, PortfolioMetrics, PortfolioMetrics> portfolioStateDescriptor =
                new AggregatingStateDescriptor<>("portfolioState", new PortfolioMetricsAggregateFunction(), PortfolioMetrics.class);
        portfolioState = getRuntimeContext().getAggregatingState(portfolioStateDescriptor);
    }

    @Override
    public void processElement1(AssetTrade assetTrade, CoProcessFunction<AssetTrade, PriceUpdate, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> collector) throws Exception {
        portfolioState.add(Tuple2.of(assetTrade, null));
        PortfolioMetrics portfolioMetrics = portfolioState.get();
        collector.collect(Tuple2.of(assetTrade.getAsset(), portfolioMetrics.getValueToCostRatio()));
    }

    @Override
    public void processElement2(PriceUpdate priceUpdate, CoProcessFunction<AssetTrade, PriceUpdate, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> collector) throws Exception {
        portfolioState.add(Tuple2.of(null, priceUpdate));
        PortfolioMetrics portfolioMetrics = portfolioState.get();
        collector.collect(Tuple2.of(priceUpdate.getAsset(), portfolioMetrics.getValueToCostRatio()));
    }
}
