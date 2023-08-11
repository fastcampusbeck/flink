package com.fastcampus.streaming.flinkcourse.chapter4.mapstate;

import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.database.DatabaseQuery;
import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.function.TransactionWithRiskCoMapFunction;
import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source.RiskUpdateSource;
import com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source.TradeWithCustomerSource;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithRisk;
import com.fastcampus.streaming.flinkcourse.model.asset.RiskUpdate;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class MapStatePractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AssetTradeWithCustomer> tradesWithCustomer = env.addSource(new TradeWithCustomerSource());

        DataStream<RiskUpdate> riskUpdates = env.addSource(new RiskUpdateSource());
        DataStream<RiskUpdate> broadcastRiskUpdates = riskUpdates.broadcast();

        ConnectedStreams<AssetTradeWithCustomer, RiskUpdate> connectedStreams = tradesWithCustomer.connect(broadcastRiskUpdates);
        DataStream<AssetTradeWithRisk> assetsTradeWithRisk = connectedStreams.map(new TransactionWithRiskCoMapFunction())
                .filter(Objects::nonNull);

        AsyncDataStream.unorderedWait(assetsTradeWithRisk,
                        new DatabaseQuery(), 1000, TimeUnit.MILLISECONDS, 100)
                .print();

        env.execute("RiskWeightCalculation");
    }
}
