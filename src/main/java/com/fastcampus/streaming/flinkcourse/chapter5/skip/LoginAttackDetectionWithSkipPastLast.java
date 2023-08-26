package com.fastcampus.streaming.flinkcourse.chapter5.skip;

import com.fastcampus.streaming.flinkcourse.chapter5.source.WebEventSource;
import com.fastcampus.streaming.flinkcourse.model.cep.WebAlert;
import com.fastcampus.streaming.flinkcourse.model.cep.WebEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoginAttackDetectionWithSkipPastLast {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WebEvent> loginEvents = env.addSource(new WebEventSource());

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        Pattern<WebEvent, ?> loginAttackPattern = Pattern.<WebEvent>begin("start", skipStrategy)
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(WebEvent value) {
                        return value.getType().equals("LOGIN_FAILURE");
                    }
                })
                .timesOrMore(3).consecutive()
                .followedBy("middle")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(WebEvent value) {
                        return value.getType().equals("LOGIN_SUCCESS");
                    }
                })
                .followedBy("end")
                .where(new IterativeCondition<WebEvent>() {
                    @Override
                    public boolean filter(WebEvent value, Context<WebEvent> ctx) {
                        return value.getType().equals("UNUSUAL_ACTIVITY");
                    }
                })
                .within(Time.minutes(1));

        PatternStream<WebEvent> patternStream = CEP.pattern(loginEvents, loginAttackPattern);

        DataStream<WebAlert> alerts = patternStream.inProcessingTime().process(new PatternProcessFunction<>() {
            @Override
            public void processMatch(Map<String, List<WebEvent>> pattern, Context ctx, Collector<WebAlert> out) throws Exception {
                int index = pattern.get("start").size(); // start 이벤트의 LOGIN_FAILURE 수를 인덱스로 사용
                String message = String.format("Pattern of size %d: Possible attack detected", index);
                out.collect(new WebAlert(message, pattern));
            }
        });

        alerts.print();

        env.execute("Login Attack Detection with Skip Past Last");
    }
}
