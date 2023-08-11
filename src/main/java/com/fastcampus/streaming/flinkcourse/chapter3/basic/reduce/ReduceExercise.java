package com.fastcampus.streaming.flinkcourse.chapter3.basic.reduce;

import com.fastcampus.streaming.flinkcourse.chapter3.basic.reduce.function.TotalVolumeReduceFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class ReduceExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("reduce-exercise")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-transaction-kafka-source");

        DataStream<StockTransaction> stockTotalVolume = stockTransactions
                .keyBy(StockTransaction::getSymbol)
                .reduce(new TotalVolumeReduceFunction());

        stockTotalVolume.print();

        env.execute("Stock Total Volume");
    }
}
