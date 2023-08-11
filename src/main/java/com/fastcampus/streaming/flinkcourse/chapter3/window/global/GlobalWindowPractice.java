package com.fastcampus.streaming.flinkcourse.chapter3.window.global;

import com.fastcampus.streaming.flinkcourse.chapter3.window.global.evictor.PortfolioValidTimestampEvictor;
import com.fastcampus.streaming.flinkcourse.chapter3.window.global.function.VaRCalculatorWindowFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.window.global.trigger.PortfolioTrigger;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaProperties;
import com.fastcampus.streaming.flinkcourse.kafka.config.KafkaSourceSink;
import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import com.fastcampus.streaming.flinkcourse.model.portfolio.VaR;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.time.Duration;
import java.util.Properties;

public class GlobalWindowPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new KafkaProperties()
                .setSourceTopic("portfolios")
                .setGroupId("global-window-practice")
                .build();

        KafkaSource<Portfolio> kafkaSource = KafkaSourceSink.createSource(properties, Portfolio.class);

        DataStream<Portfolio> portfolios = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .<Portfolio>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()).withIdleness(Duration.ofMillis(500)),
                "portfolio-kafka-source");

        portfolios.map(Portfolio::getId)
                .print();

        DataStream<VaR> vaRs = portfolios
                .keyBy(Portfolio::getId)
                .window(GlobalWindows.create())
                .trigger(new PortfolioTrigger())
                .evictor(new PortfolioValidTimestampEvictor())
                .apply(new VaRCalculatorWindowFunction());

        vaRs.print();

        env.execute("Portfolio Value at Risk (VaR) Calculation");
    }
}
