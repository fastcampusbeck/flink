package com.fastcampus.streaming.flinkcourse.chapter5.combining;

import com.fastcampus.streaming.flinkcourse.chapter5.source.TransactionEventSource;
import com.fastcampus.streaming.flinkcourse.model.cep.TransactionEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class WithdrawalWithinSpecificTime {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TransactionEvent> transactionStream = env.addSource(new TransactionEventSource());

        Pattern<TransactionEvent, ?> combinedPattern = Pattern.<TransactionEvent>begin("firstWithdrawal")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(TransactionEvent value) {
                        return value.getAmount() >= 5000;
                    }
                })
                .followedBy("secondWithdrawal")
                .where(new IterativeCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent second, Context<TransactionEvent> ctx) throws Exception {
                        for (TransactionEvent first: ctx.getEventsForPattern("firstWithdrawal")) {
                            if (first.getUserId().equals(second.getUserId()) && second.getAmount() <= 2000) {
                                return true;
                            }
                        }
                        return false;
                    }
                })
                .within(Time.seconds(10));

        PatternStream<TransactionEvent> patternStream = CEP.pattern(transactionStream, combinedPattern);

        DataStream<String> result = patternStream.inProcessingTime().process(new PatternProcessFunction<TransactionEvent, String>() {
            @Override
            public void processMatch(Map<String, List<TransactionEvent>> pattern, Context ctx, Collector<String> out) {
                TransactionEvent firstWithdrawal = pattern.get("firstWithdrawal").get(0);
                TransactionEvent secondWithdrawal = pattern.get("secondWithdrawal").get(0);
                out.collect("Pattern detected: First = " + firstWithdrawal + ", Second = " + secondWithdrawal);
            }
        });

        result.print();

        env.execute("Combining Patterns with PatternProcessFunction Example");
    }
}
