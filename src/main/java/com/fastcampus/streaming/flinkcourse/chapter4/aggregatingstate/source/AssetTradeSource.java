package com.fastcampus.streaming.flinkcourse.chapter4.aggregatingstate.source;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTrade;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AssetTradeSource extends RichParallelSourceFunction<AssetTrade> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    @Override
    public void run(SourceFunction.SourceContext<AssetTrade> sourceContext) throws Exception {
        String[] assetTypes = {"Stock", "Bond", "Real Estate", "Cryptocurrency"};
        while (isRunning) {
            for (String assetType : assetTypes) {
                // Generate a random asset name (e.g., "Stock-42") for each asset type.
                String assetName = assetType + "-" + random.nextInt(100);
                boolean isBuy = random.nextBoolean();
                double price = random.nextDouble() * 1000 + 1;
                int amount = random.nextInt(10) + 1;

                sourceContext.collect(new AssetTrade(assetName, isBuy, price, amount));
            }
            // Wait for a second before the next trade.
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
