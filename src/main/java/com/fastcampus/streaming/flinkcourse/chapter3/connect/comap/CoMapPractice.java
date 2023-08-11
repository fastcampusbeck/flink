package com.fastcampus.streaming.flinkcourse.chapter3.connect.comap;

import com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.function.RiskAlertCoMapFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.function.RiskScoreRichMapFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.prediction.RiskScore;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class CoMapPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("co-map-practice")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "stock-transaction-kafka-source");
        stockTransactions.print();

        DataStream<RiskScore> riskScores = stockTransactions.map(new RiskScoreRichMapFunction());

        ConnectedStreams<StockTransaction, RiskScore> connectedStreams = stockTransactions.connect(riskScores);

        DataStream<String> alerts = connectedStreams
                .keyBy(StockTransaction::getSymbol, RiskScore::getSymbol)
                .map(new RiskAlertCoMapFunction())
                .filter(s -> !s.isBlank());
        alerts.print();

        env.execute("High Risk Trade Alert Job");
    }
}
