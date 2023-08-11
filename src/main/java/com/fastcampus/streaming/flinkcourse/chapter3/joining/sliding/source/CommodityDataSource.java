package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.source;

import com.fastcampus.streaming.flinkcourse.model.commodity.Commodity;
import com.fastcampus.streaming.flinkcourse.model.commodity.GoldCommodity;
import com.fastcampus.streaming.flinkcourse.model.commodity.SilverCommodity;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class CommodityDataSource extends RichParallelSourceFunction<Commodity> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    @Override
    public void run(SourceContext<Commodity> sourceContext) throws Exception {
        while (isRunning) {
            String exchange = "Exchange" + random.nextInt(10);
            long timestamp = System.currentTimeMillis();
            double goldPrice = 1000 + random.nextDouble() * 500; // Gold price range: $1000 - $1500
            double silverPrice = 10 + random.nextDouble() * 20; // Silver price range: $10 - $30

            sourceContext.collect(new GoldCommodity(exchange, goldPrice, timestamp));
            sourceContext.collect(new SilverCommodity(exchange, silverPrice, timestamp));

            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

