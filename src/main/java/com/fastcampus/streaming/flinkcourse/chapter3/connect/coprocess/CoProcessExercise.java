package com.fastcampus.streaming.flinkcourse.chapter3.connect.coprocess;

import com.fastcampus.streaming.flinkcourse.chapter3.connect.coprocess.function.SideOutputCoProcessFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.connect.coprocess.source.StockNewsSource;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.news.StockNews;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;

public class CoProcessExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<String> lowImportanceTag = new OutputTag<>("lowImportanceAlerts") {};
        OutputTag<String> mediumImportanceTag = new OutputTag<>("mediumImportanceAlerts") {};
        OutputTag<String> highImportanceTag = new OutputTag<>("highImportanceAlerts") {};

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("co-flat-map-practice")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource, WatermarkStrategy.<StockTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((stock, timestamp) -> stock.getTimestamp())
                        .withIdleness(Duration.ofMillis(500)),
                "stock-transaction-kafka-source");

        DataStream<StockNews> stockNews = env.addSource(new StockNewsSource());

        SingleOutputStreamOperator<String> result = stockTransactions
                .connect(stockNews)
                .process(new SideOutputCoProcessFunction());

        DataStream<String> lowImportanceInfo = result.getSideOutput(lowImportanceTag);
        lowImportanceInfo.print();
        DataStream<String> mediumImportanceInfo = result.getSideOutput(mediumImportanceTag);
        mediumImportanceInfo.print();
        DataStream<String> highImportanceInfo = result.getSideOutput(highImportanceTag);
        highImportanceInfo.print();

        env.execute("Select importance through StockNews");
    }
}
