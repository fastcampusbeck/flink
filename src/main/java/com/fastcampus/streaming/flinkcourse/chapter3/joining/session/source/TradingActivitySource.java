package com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source;

import com.fastcampus.streaming.flinkcourse.model.trade.Trade;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public class TradingActivitySource extends RichSourceFunction<Trade> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String exchange;

    public TradingActivitySource(String exchange) {
        this.exchange = exchange;
    }

    @Override
    public void run(SourceContext<Trade> sourceContext) throws Exception {
        while (isRunning) {
            Trade trade = new Trade();
            trade.setExchange(exchange);
            trade.setTimestamp(System.currentTimeMillis());
            trade.setTradeVolume(random.nextDouble() * 1000);

            sourceContext.collect(trade);

            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
