package com.fastcampus.streaming.flinkcourse.chapter5.individual;

import com.fastcampus.streaming.flinkcourse.chapter5.source.TransactionEventSource;
import com.fastcampus.streaming.flinkcourse.model.cep.TransactionEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class WithdrawalLargeAmounts {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TransactionEvent> transactionStream = env.addSource(new TransactionEventSource());

        Pattern<TransactionEvent, ?> largeWithdrawalPattern = Pattern.<TransactionEvent>begin("largeWithdrawal")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(TransactionEvent value) {
                        return value.getAmount() >= 10000;
                    }
                });

        PatternStream<TransactionEvent> patternStream = CEP.pattern(transactionStream, largeWithdrawalPattern);

        DataStream<String> result = patternStream.inProcessingTime().process(new PatternProcessFunction<>() {
            @Override
            public void processMatch(Map<String, List<TransactionEvent>> pattern, Context ctx, Collector<String> out) {
                TransactionEvent largeWithdrawal = pattern.get("largeWithdrawal").get(0);
                out.collect("Large withdrawal detected: " + largeWithdrawal);
            }
        });

        result.print();

        env.execute("Individual Pattern Example");
    }
}
