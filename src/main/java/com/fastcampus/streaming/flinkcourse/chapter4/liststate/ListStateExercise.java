package com.fastcampus.streaming.flinkcourse.chapter4.liststate;

import com.fastcampus.streaming.flinkcourse.chapter4.liststate.function.MovingAverageKeyedProcessFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class ListStateExercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("list-state-exercise")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource, WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((stock, timestamp) -> stock.getTimestamp())
                        .withIdleness(Duration.ofSeconds(1)), "stock-kafka-source");

        stocks.keyBy(Stock::getSymbol)
                .process(new MovingAverageKeyedProcessFunction(5))
                .print();

        env.execute("ListState Exercise");
    }
}
