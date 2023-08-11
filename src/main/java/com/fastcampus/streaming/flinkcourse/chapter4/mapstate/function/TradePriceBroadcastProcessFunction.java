package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import com.fastcampus.streaming.flinkcourse.model.trade.PriceUpdateEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

import static com.fastcampus.streaming.flinkcourse.chapter4.mapstate.MapStateExercise.priceUpdateDescriptor;

public class TradePriceBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, AssetTradeWithCustomer, PriceUpdateEvent, Tuple3<String, String, String>> {
    private volatile MapState<String, Tuple2<Double, Double>> assetHoldingsState; // asset -> (quantity, cost)

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Tuple2<Double, Double>> descriptor =
                new MapStateDescriptor<>("assetHoldings", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<>() {}));
        assetHoldingsState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(AssetTradeWithCustomer assetTradeWithCustomer, KeyedBroadcastProcessFunction<String, AssetTradeWithCustomer, PriceUpdateEvent, Tuple3<String, String, String>>.ReadOnlyContext readOnlyContext, Collector<Tuple3<String, String, String>> collector) throws Exception {
        Tuple2<Double, Double> assetHolding = assetHoldingsState.get(assetTradeWithCustomer.getAsset());

        if (!assetTradeWithCustomer.getIsBuy()) {
            if (assetHolding != null && assetHolding.f0 >= assetTradeWithCustomer.getAmount()) {
                double updatedQuantity = assetHolding.f0 - assetTradeWithCustomer.getAmount();
                double updatedCost = (assetHolding.f1 / assetHolding.f0) * assetTradeWithCustomer.getPrice();
                assetHoldingsState.put(assetTradeWithCustomer.getAsset(),
                        Tuple2.of(updatedQuantity, (assetHolding.f1 / assetHolding.f0) * assetTradeWithCustomer.getPrice()));

                String message = "Sold " + assetTradeWithCustomer.getAmount() + " of " + assetTradeWithCustomer.getAsset()
                        + " at " + assetTradeWithCustomer.getPrice();
                collector.collect(Tuple3.of(assetTradeWithCustomer.getCustomerId(), message, "Remaining: " + updatedCost));
            }
        } else {
            double newQuantity = assetHolding != null ? assetHolding.f0 + assetTradeWithCustomer.getAmount() : assetTradeWithCustomer.getAmount();
            double newCost = assetHolding != null ?
                    assetHolding.f1 + assetTradeWithCustomer.getPrice() * assetTradeWithCustomer.getAmount() :
                    assetTradeWithCustomer.getPrice() * assetTradeWithCustomer.getAmount();
            assetHoldingsState.put(assetTradeWithCustomer.getAsset(), Tuple2.of(newQuantity, newCost));

            String message = "Bought " + assetTradeWithCustomer.getAmount() + " of " + assetTradeWithCustomer.getAsset()
                    + " at " + assetTradeWithCustomer.getPrice();
            collector.collect(Tuple3.of(assetTradeWithCustomer.getCustomerId(), message, "Total: " + newCost));
        }
    }

    @Override
    public void processBroadcastElement(PriceUpdateEvent priceUpdateEvent, KeyedBroadcastProcessFunction<String, AssetTradeWithCustomer, PriceUpdateEvent, Tuple3<String, String, String>>.Context context, Collector<Tuple3<String, String, String>> collector) {
        try {
            context.getBroadcastState(priceUpdateDescriptor).put(priceUpdateEvent.getAsset(), priceUpdateEvent.getPrice());

            for (Map.Entry<String, Tuple2<Double, Double>> entry : assetHoldingsState.entries()) {
                String asset = entry.getKey();
                double quantity = entry.getValue().f0;
                double cost = entry.getValue().f1;

                if (asset.equals(priceUpdateEvent.getAsset()) && quantity > 0) {
                    double currentValue = quantity * priceUpdateEvent.getPrice();
                    String message = currentValue > cost ? "Profitable" : "Not profitable";
                    collector.collect(Tuple3.of(asset, "Current status:", message));
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred during the processing of the broadcast element at " + priceUpdateEvent.getAsset() + " : " + e.getMessage());
        }
    }
}
