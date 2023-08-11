package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function;


import com.fastcampus.streaming.flinkcourse.model.user.LoginEvent;
import com.fastcampus.streaming.flinkcourse.model.user.TradeEvent;
import com.fastcampus.streaming.flinkcourse.model.user.UserActivity;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class LoginEventJoinFunction extends ProcessJoinFunction<TradeEvent, LoginEvent, UserActivity> {
    @Override
    public void processElement(TradeEvent tradeEvent, LoginEvent loginEvent, ProcessJoinFunction<TradeEvent, LoginEvent, UserActivity>.Context context, Collector<UserActivity> collector) throws Exception {
        collector.collect(new UserActivity(tradeEvent.getUserId(), tradeEvent.getSessionId(), "Trade after login", tradeEvent.getTimestamp()));
    }
}
