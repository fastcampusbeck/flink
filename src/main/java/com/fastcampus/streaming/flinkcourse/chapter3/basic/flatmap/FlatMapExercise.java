package com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap;

import com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap.function.StockFlatMapFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlatMapExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-groups")
                .setGroupId("flatMap-exercise")
                .build();

        KafkaSource<String> kafkaSource = KafkaSourceSink.createSource(properties, String.class);

        DataStream<String> stockGroups = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "stock-group-kafka-source");

        DataStream<Stock> stocks = stockGroups.flatMap(new StockFlatMapFunction());

        stocks.print();

        env.execute("Stock FlatMap");
    }
}
