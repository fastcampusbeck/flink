package com.fastcampus.streaming.flinkcourse.chapter3.window.global;

import com.fastcampus.streaming.flinkcourse.chapter3.window.global.conversion.GlobalWindowExerciseKeyedProcessFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.window.global.function.TotalTransactionAmountCalculatorProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

import java.util.Properties;

public class GlobalWindowExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("global-window-exercise")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-transaction-kafka-source");

        DataStream<TotalTradedVolume> stockTotalVolume = stockTransactions
                .keyBy(StockTransaction::getSymbol)
                .window(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(10)))
                .process(new TotalTransactionAmountCalculatorProcessWindowFunction());

//        stockTotalVolume.print();

        stockTransactions.keyBy(StockTransaction::getSymbol)
                .process(new GlobalWindowExerciseKeyedProcessFunction())
                .print();

        env.execute("Real-time Session Volume Calculation");
    }
}
