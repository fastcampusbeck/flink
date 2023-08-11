package com.fastcampus.streaming.flinkcourse.chapter3.iterate;

import com.fastcampus.streaming.flinkcourse.chapter3.iterate.function.CalculatePageRankBroadcastProcessFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.iterate.source.PageRankSource;
import com.fastcampus.streaming.flinkcourse.chapter3.iterate.source.StockCorrelationSource;
import com.fastcampus.streaming.flinkcourse.model.stock.StockCorrelation;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class iteratePractice {
    public static final MapStateDescriptor<String, List<StockCorrelation>> correlationsDescriptor = new MapStateDescriptor<>(
            "correlations", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<>() {}));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Map<String, Double>> pageRanks = env.addSource(new PageRankSource());

        DataStream<StockCorrelation> stockCorrelations = env.addSource(new StockCorrelationSource());
        BroadcastStream<StockCorrelation> broadcastCorrelations = stockCorrelations.broadcast(correlationsDescriptor);

        IterativeStream<Map<String, Double>> iteration = pageRanks
                .iterate();

        DataStream<Map<String, Double>> iterationBody = iteration
                .connect(broadcastCorrelations)
                .process(new CalculatePageRankBroadcastProcessFunction());

        iteration.closeWith(iterationBody)
                .print();

        env.execute("Stock PageRank with Broadcast");
    }
}
