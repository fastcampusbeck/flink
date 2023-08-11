package com.fastcampus.streaming.flinkcourse.chapter4.liststate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetPrice;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class PortfolioAnalysisKeyedProcessFunction extends KeyedProcessFunction<String, AssetPrice, String> {
    private transient ListState<Tuple2<Long, Double>> priceState;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Tuple2<Long, Double>> descriptor = new ListStateDescriptor<>(
                "assetPrices", TypeInformation.of(new TypeHint<>() {
        }));
        priceState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(AssetPrice assetPrice, KeyedProcessFunction<String, AssetPrice, String>.Context context, Collector<String> collector) throws Exception {
        Iterable<Tuple2<Long, Double>> currentPrices = priceState.get();
        PriorityQueue<Tuple2<Long, Double>> prices = new PriorityQueue<>(
                Comparator.comparing(longDoubleTuple2 -> longDoubleTuple2.f0)
        );
        currentPrices.forEach(prices::add);

        prices.add(Tuple2.of(assetPrice.getTimestamp(), assetPrice.getPrice()));

        PriorityQueue<Tuple2<Long, Double>> pricesForPrint = new PriorityQueue<>(prices);
        while (!pricesForPrint.isEmpty()) {
            System.out.println(pricesForPrint.poll());
        }

        if (prices.size() == 5) {
            prices.poll();
        }

        List<Tuple2<Long, Double>> updatedPrices = new ArrayList<>(prices);
        priceState.update(updatedPrices);

        double sum = prices.stream().mapToDouble(price -> price.f1).sum();
        double average = sum / prices.size();

        collector.collect("Asset: " + assetPrice.getAssetName() +
                ", Portfolio: " + assetPrice.getPortfolioId() +
                ", Average of Last 5 Prices: " + average);
    }
}
