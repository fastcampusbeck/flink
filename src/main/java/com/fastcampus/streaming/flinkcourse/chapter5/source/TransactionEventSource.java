package com.fastcampus.streaming.flinkcourse.chapter5.source;

import com.fastcampus.streaming.flinkcourse.model.cep.TransactionEvent;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class TransactionEventSource extends RichSourceFunction<TransactionEvent> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<TransactionEvent> ctx) throws Exception {
        String[] userIds = {"user1", "user2", "user3"};
        while (isRunning) {
            for (String userId : userIds) {
                ctx.collect(new TransactionEvent(userId, Math.random() * 20000));  // $0 ~ $20,000
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}