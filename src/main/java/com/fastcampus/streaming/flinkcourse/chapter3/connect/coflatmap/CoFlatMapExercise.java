package com.fastcampus.streaming.flinkcourse.chapter3.connect.coflatmap;

import com.fastcampus.streaming.flinkcourse.chapter3.connect.coflatmap.function.BotRecommendationRichCoFlatMapFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.connect.coflatmap.source.BotDirectiveSource;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.recommendation.BotDirective;
import com.fastcampus.streaming.flinkcourse.model.recommendation.StockRecommendation;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class CoFlatMapExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        DataStream<BotDirective> botDirectives = env.addSource(new BotDirectiveSource());

        ConnectedStreams<StockTransaction, BotDirective> connectedStreams = stockTransactions.connect(botDirectives);

        DataStream<StockRecommendation> recommendations = connectedStreams
                .keyBy(StockTransaction::getSymbol, BotDirective::getSymbol)
                .flatMap(new BotRecommendationRichCoFlatMapFunction());

        recommendations.print();

        env.execute("CoFlatMap Practice");
    }
}
