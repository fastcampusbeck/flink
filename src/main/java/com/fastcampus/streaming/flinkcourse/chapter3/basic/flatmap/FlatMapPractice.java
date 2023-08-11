package com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap;

import com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap.function.SectorStockFlatMapFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.SectorStock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlatMapPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-sectors")
                .setGroupId("flatMap-practice")
                .build();

        KafkaSource<String> kafkaSource = KafkaSourceSink.createSource(properties, String.class);

        DataStream<String> stockSectors = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "sector-stock-kafka-source");

        DataStream<SectorStock> sectorStocks = stockSectors.flatMap(new SectorStockFlatMapFunction());

        sectorStocks.print();

        env.execute("Stock Price In EUR");
    }
}
