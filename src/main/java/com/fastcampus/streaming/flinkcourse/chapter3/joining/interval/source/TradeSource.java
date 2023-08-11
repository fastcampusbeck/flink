package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.source;

import com.fastcampus.streaming.flinkcourse.model.trade.Trade;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class TradeSource extends RichSourceFunction<Trade> {

    private volatile boolean isRunning = true;
    private static final Random RANDOM = new Random();
    private static final String[] SECURITIES = {"sec1", "sec2", "sec3", "sec4", "sec5"};

    @Override
    public void run(SourceFunction.SourceContext<Trade> ctx) throws Exception {
        while (isRunning) {
            String exchange = "Exchange-" + (RANDOM.nextInt(10) + 1);
            String securityId = SECURITIES[RANDOM.nextInt(SECURITIES.length)];
            long timestamp = System.currentTimeMillis();
            double tradePrice = RANDOM.nextDouble() * 100;
            double tradeVolume = RANDOM.nextDouble() * 1000;

            Trade trade = new Trade(exchange, securityId, timestamp, tradePrice, tradeVolume);

            ctx.collect(trade);

            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}