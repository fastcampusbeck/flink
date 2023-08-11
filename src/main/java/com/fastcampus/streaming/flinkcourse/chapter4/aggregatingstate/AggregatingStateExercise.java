package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate;

import com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.function.ValueToCostRatioCoProcessFunction;
import com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.source.AssetTradeSource;
import com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.source.PriceUpdateSource;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetTrade;
import com.fastcampus.streaming.flinkcourse.model.asset.PriceUpdate;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregatingStateExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AssetTrade> assetTrades = env.addSource(new AssetTradeSource());
        DataStream<PriceUpdate> priceUpdates = env.addSource(new PriceUpdateSource());

        ConnectedStreams<AssetTrade, PriceUpdate> connectedStream = assetTrades.connect(priceUpdates);

        connectedStream.keyBy(AssetTrade::getAsset, PriceUpdate::getAsset)
                .process(new ValueToCostRatioCoProcessFunction())
                .print();

        env.execute();
    }
}
