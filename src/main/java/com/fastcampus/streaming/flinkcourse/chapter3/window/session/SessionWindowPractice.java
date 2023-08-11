package com.fastcampus.streaming.flinkcourse.chapter3.window.session;

import com.fastcampus.streaming.flinkcourse.chapter3.window.session.function.SessionTotalVolumeCalculatorProcessWindowFunction;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import com.fastcampus.streaming.flinkcourse.util.DateTimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;

public class SessionWindowPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("stock-transactions")
                .setGroupId("session-window-practice")
                .build();

        KafkaSource<StockTransaction> kafkaSource = KafkaSourceSink.createSource(properties, StockTransaction.class);

        DataStream<StockTransaction> stockTransactions = env.fromSource(
                kafkaSource, WatermarkStrategy.<StockTransaction>forBoundedOutOfOrderness(Duration.ofMillis(500))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                        .withIdleness(Duration.ofMillis(250)),
                "stock-transaction-kafka-source");

        DataStream<Tuple5<String, Long, Long, Long, Long>> stockTotalVolume = stockTransactions
                .keyBy(StockTransaction::getSymbol)
                .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
                .process(new SessionTotalVolumeCalculatorProcessWindowFunction());

//        stockTotalVolume.print();

        stockTotalVolume.map(stringLongLongLongLongTuple5 ->
                new Tuple4<>(stringLongLongLongLongTuple5.f0,
                        DateTimeConverter.epochMilliToZonedDateTime(stringLongLongLongLongTuple5.f1, ZoneId.systemDefault()).toString(),
                        DateTimeConverter.epochMilliToZonedDateTime(stringLongLongLongLongTuple5.f2, ZoneId.systemDefault()).toString(),
                        DateTimeConverter.epochMilliToZonedDateTime(stringLongLongLongLongTuple5.f3, ZoneId.systemDefault()).toString()))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING))
                .print();

        env.execute("Real-time Session Volume Calculation");
    }
}
