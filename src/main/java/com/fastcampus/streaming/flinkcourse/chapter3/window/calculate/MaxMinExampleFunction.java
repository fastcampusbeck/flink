package com.fastcampus.streaming.flinkcourse.chapter3.window.calculate;

import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.utils.SystemTime;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.TimeZone;

public class MaxMinExampleFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("max-min-window-function")
                .build();

        KafkaSource<Stock> kafkaSource = KafkaSourceSink.createSource(properties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofSeconds(3)),
                "stock-kafka-source");

        stocks.keyBy(Stock::getSymbol)
                .process(new KeyedProcessFunction<String, Stock, Stock>() {
                    @Override
                    public void processElement(Stock stock, KeyedProcessFunction<String, Stock, Stock>.Context context, Collector<Stock> collector) throws Exception {
                        long currentWatermark = context.timerService().currentWatermark();
                        System.out.println("Current Watermark: " + currentWatermark);
                        collector.collect(stock);
                    }
                });

        stocks.keyBy(Stock::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .max("price")
                .name("Max Stock Price")
                .print();

        stocks.keyBy(Stock::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .min("price")
                .name("Min Stock Price")
                .print();

        env.execute("Real-time Stock Price Analysis (Max, Min)");
    }
}
