package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function;


import com.fastcampus.streaming.flinkcourse.model.user.UserActivity;
import com.fastcampus.streaming.flinkcourse.model.user.ViewEvent;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class ViewEventJoinFunction extends ProcessJoinFunction<UserActivity, ViewEvent, UserActivity> {
    @Override
    public void processElement(UserActivity userActivity, ViewEvent viewEvent, ProcessJoinFunction<UserActivity, ViewEvent, UserActivity>.Context context, Collector<UserActivity> collector) throws Exception {
        userActivity.addActivity("Viewed " + viewEvent.getStockId());
        userActivity.setTimestamp(viewEvent.getTimestamp());
        collector.collect(userActivity);
    }
}
