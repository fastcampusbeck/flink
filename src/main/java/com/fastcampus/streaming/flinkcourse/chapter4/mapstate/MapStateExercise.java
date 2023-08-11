package com.fastcampus.streaming.flinkcourse.chapter4.mapstate;

import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.function.TradePriceBroadcastProcessFunction;
import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source.PriceUpdateEventSource;
import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source.TradeWithCustomerSource;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import com.fastcampus.streaming.flinkcourse.model.trade.PriceUpdateEvent;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapStateExercise {
    public static MapStateDescriptor<String, Double> priceUpdateDescriptor = new MapStateDescriptor<>(
            "PriceUpdates", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<>() {}));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AssetTradeWithCustomer> tradesWithCustomer = env.addSource(new TradeWithCustomerSource());
        DataStream<PriceUpdateEvent> priceUpdates = env.addSource(new PriceUpdateEventSource());

        BroadcastStream<PriceUpdateEvent> broadcastedPriceUpdates = priceUpdates.broadcast(priceUpdateDescriptor);

        tradesWithCustomer.keyBy(AssetTradeWithCustomer::getCustomerId)
                .connect(broadcastedPriceUpdates)
                .process(new TradePriceBroadcastProcessFunction())
                .print();

        env.execute();
    }
}
