package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.source;

import com.fastcampus.streaming.flinkcourse.model.asset.PriceUpdate;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class PriceUpdateSource extends RichParallelSourceFunction<PriceUpdate> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private static final String[] ASSET_TYPES = {"Stock", "Bond", "Real Estate", "Cryptocurrency"};


    @Override
    public void run(SourceContext<PriceUpdate> sourceContext) throws Exception {
        while (isRunning) {
            for (String assetType : ASSET_TYPES) {
                // Generate a random asset name (e.g., "Stock-42") for each asset type.
                String assetName = assetType + "-" + random.nextInt(100);
                double price = (random.nextDouble() - 0.5) * 100;

                sourceContext.collect(new PriceUpdate(assetName, price));
            }
            // Wait for a second before the next update.
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
