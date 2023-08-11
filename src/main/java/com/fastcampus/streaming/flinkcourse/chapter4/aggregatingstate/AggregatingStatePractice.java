package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate;

import com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.function.CalculateNetProfitFlatMapFunction;
import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source.TradeWithCustomerSource;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregatingStatePractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AssetTradeWithCustomer> tradesWithCustomer = env.addSource(new TradeWithCustomerSource());

        DataStream<Tuple2<String, Double>> netProfitPerCustomer = tradesWithCustomer
                .keyBy(AssetTradeWithCustomer::getCustomerId)
                .flatMap(new CalculateNetProfitFlatMapFunction());

        netProfitPerCustomer.print();

        env.execute("Aggregating State Practice");
    }
}
