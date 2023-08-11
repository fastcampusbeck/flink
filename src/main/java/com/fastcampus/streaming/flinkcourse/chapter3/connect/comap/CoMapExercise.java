package com.fastcampus.streaming.flinkcourse.chapter3.connect.comap;

import com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.function.PriceComparisonRichCoMapFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.source.StockPredictionSource;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockPrediction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class CoMapExercise {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties stockProperties = new KafkaProperties()
                .setSourceTopic("stocks")
                .setGroupId("co-map-exercise")
                .build();

        KafkaSource<Stock> stockKafkaSource = KafkaSourceSink.createSource(stockProperties, Stock.class);

        DataStream<Stock> stocks = env.fromSource(
                stockKafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "stock-kafka-source");

        DataStream<StockPrediction> stockPredictions = env.addSource(new StockPredictionSource());

        DataStream<String> resultStream = stocks.connect(stockPredictions)
                .keyBy("symbol", "symbol")
                .map(new PriceComparisonRichCoMapFunction());

        resultStream.print();

        env.execute("Price Comparison");
    }
}
