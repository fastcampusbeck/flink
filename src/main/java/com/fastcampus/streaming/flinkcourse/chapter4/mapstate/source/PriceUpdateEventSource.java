package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source;

import com.fastcampus.streaming.flinkcourse.model.trade.PriceUpdateEvent;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;

public class PriceUpdateEventSource extends RichSourceFunction<PriceUpdateEvent> {

    private volatile boolean isRunning = true;
    private static final Random RANDOM = new Random();

    private static final String[] ASSETS = {"Stocks", "Bonds", "Commodities", "Real Estate", "Cryptocurrency"};

    @Override
    public void run(SourceContext<PriceUpdateEvent> sourceContext) throws Exception {
        while (isRunning) {
            // Generate PriceUpdateEvent
            PriceUpdateEvent priceUpdateEvent = new PriceUpdateEvent();
            priceUpdateEvent.setAsset(ASSETS[RANDOM.nextInt(ASSETS.length)]);
            priceUpdateEvent.setPrice(50 + RANDOM.nextDouble() * 150);

            sourceContext.collectWithTimestamp(priceUpdateEvent, System.currentTimeMillis());
            sourceContext.emitWatermark(new Watermark(System.currentTimeMillis() - 1));

            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}