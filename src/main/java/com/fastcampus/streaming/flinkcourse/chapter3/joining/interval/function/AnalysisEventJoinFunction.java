package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function;


import com.fastcampus.streaming.flinkcourse.model.user.AnalysisEvent;
import com.fastcampus.streaming.flinkcourse.model.user.UserActivity;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class AnalysisEventJoinFunction  extends ProcessJoinFunction<UserActivity, AnalysisEvent, UserActivity> {
    @Override
    public void processElement(UserActivity userActivity, AnalysisEvent analysisEvent, ProcessJoinFunction<UserActivity, AnalysisEvent, UserActivity>.Context context, Collector<UserActivity> collector) throws Exception {
        userActivity.addActivity("Analysis performed: " + analysisEvent.getAnalysisResult());
        userActivity.setTimestamp(analysisEvent.getTimestamp());
        collector.collect(userActivity);
    }
}