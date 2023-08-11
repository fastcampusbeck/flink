package com.fastcampus.streaming.flinkcourse.chapter3.basic.reduce;

import com.fastcampus.streaming.flinkcourse.chapter3.basic.reduce.function.MaxVolumeReduceFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class ReducePractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("reduce-practice")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-transaction-kafka-source");

        DataStream<StockTransaction> stockMaxVolume = stockTransactions
                .keyBy(StockTransaction::getSymbol)
                .reduce(new MaxVolumeReduceFunction());

        stockMaxVolume.print();

        env.execute("Stock Max Volume");
    }
}
