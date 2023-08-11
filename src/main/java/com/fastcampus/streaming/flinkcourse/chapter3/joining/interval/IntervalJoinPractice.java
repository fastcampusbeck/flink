package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval;

import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function.AnalysisEventJoinFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function.LoginEventJoinFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function.ViewEventJoinFunction;
import com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.source.UserActivitySource;
import com.fastcampus.streaming.flinkcourse.model.user.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class IntervalJoinPractice {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> userEventStream = env.addSource(new UserActivitySource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                                .withIdleness(Duration.ofSeconds(10)));

        KeyedStream<LoginEvent, Tuple2<String, String>> keyedLoginEventStream = userEventStream
                .filter(event -> event instanceof LoginEvent)
                .map(event -> (LoginEvent) event)
                .keyBy(new KeySelector<LoginEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(LoginEvent loginEvent) {
                        return new Tuple2<>(loginEvent.getUserId(), loginEvent.getSessionId());
                    }
                });

        KeyedStream<ViewEvent, Tuple2<String, String>> keyedViewEventStream = userEventStream
                .filter(event -> event instanceof ViewEvent)
                .map(event -> (ViewEvent) event)
                .keyBy(new KeySelector<ViewEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(ViewEvent viewEvent) {
                        return new Tuple2<>(viewEvent.getUserId(), viewEvent.getSessionId());
                    }
                });

        KeyedStream<AnalysisEvent, Tuple2<String, String>> keyedAnalysisEventStream = userEventStream
                .filter(event -> event instanceof AnalysisEvent)
                .map(event -> (AnalysisEvent) event)
                .keyBy(new KeySelector<AnalysisEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(AnalysisEvent analysisEvent) {
                        return new Tuple2<>(analysisEvent.getUserId(), analysisEvent.getSessionId());
                    }
                });

        KeyedStream<TradeEvent, Tuple2<String, String>> keyedTradeEventStream = userEventStream
                .filter(event -> event instanceof TradeEvent)
                .map(event -> (TradeEvent) event)
                .keyBy(new KeySelector<TradeEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(TradeEvent tradeEvent) {
                        return new Tuple2<>(tradeEvent.getUserId(), tradeEvent.getSessionId());
                    }
                });

        DataStream<UserActivity> userActivityAfterLoginJoin = keyedTradeEventStream
                .intervalJoin(keyedLoginEventStream)
                .between(Time.seconds(-60), Time.seconds(0))
                .process(new LoginEventJoinFunction());

        KeyedStream<UserActivity, Tuple2<String, String>> keyedUserActivityAfterLoginJoin = userActivityAfterLoginJoin
                .keyBy(new KeySelector<UserActivity, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(UserActivity userActivity) {
                        return new Tuple2<>(userActivity.getUserId(), userActivity.getSessionId());
                    }
                });

        DataStream<UserActivity> userActivityAfterViewJoin = keyedUserActivityAfterLoginJoin
                .intervalJoin(keyedViewEventStream)
                .between(Time.seconds(-30), Time.seconds(0))
                .process(new ViewEventJoinFunction());

        KeyedStream<UserActivity, Tuple2<String, String>> keyedUserActivityAfterViewJoin = userActivityAfterViewJoin
                .keyBy(new KeySelector<UserActivity, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(UserActivity userActivity) {
                        return new Tuple2<>(userActivity.getUserId(), userActivity.getSessionId());
                    }
                });

        DataStream<UserActivity> userActivityStream = keyedUserActivityAfterViewJoin
                .intervalJoin(keyedAnalysisEventStream)
                .between(Time.seconds(-10), Time.seconds(0))
                .process(new AnalysisEventJoinFunction());

        userActivityStream.print();

        env.execute("Interval Join Practice");
    }
}
