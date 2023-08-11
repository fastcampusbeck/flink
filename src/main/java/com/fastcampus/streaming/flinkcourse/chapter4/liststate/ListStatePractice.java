package com.fastcampus.streaming.flinkcourse.chapter4.liststate;

import com.fastcampus.streaming.flinkcourse.chapter4.liststate.function.PortfolioAnalysisKeyedProcessFunction;
import com.fastcampus.streaming.flinkcourse.chapter4.liststate.source.AssetPriceSource;
import com.fastcampus.streaming.flinkcourse.model.asset.AssetPrice;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ListStatePractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AssetPrice> assetPrices = env.addSource(new AssetPriceSource());

        assetPrices.keyBy(assetPrice -> assetPrice.getAssetName() + assetPrice.getPortfolioId())
                .process(new PortfolioAnalysisKeyedProcessFunction())
                .print();

        env.execute("ListState Practice");
    }
}
