package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.function;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import com.fastcampus.streaming.flinkcourse.model.trade.CustomerTradeStats;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CalculateNetProfitFlatMapFunction extends RichFlatMapFunction<AssetTradeWithCustomer, Tuple2<String, Double>> {
    private transient AggregatingState<AssetTradeWithCustomer, Double> netProfitState;

    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<AssetTradeWithCustomer, CustomerTradeStats, Double> netProfitDescriptor =
                new AggregatingStateDescriptor<>("netProfit", new CustomerTradeStatsAggregator(), TypeInformation.of(new TypeHint<>() {}));
        netProfitState = getRuntimeContext().getAggregatingState(netProfitDescriptor);
    }

    @Override
    public void flatMap(AssetTradeWithCustomer assetTradeWithCustomer, Collector<Tuple2<String, Double>> collector) throws Exception {
        netProfitState.add(assetTradeWithCustomer);
        collector.collect(Tuple2.of(assetTradeWithCustomer.getCustomerId(), netProfitState.get()));
    }
}