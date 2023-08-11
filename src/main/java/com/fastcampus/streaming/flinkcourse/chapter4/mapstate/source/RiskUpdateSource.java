package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source;

import com.fastcampus.streaming.flinkcourse.model.asset.RiskUpdate;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;

public class RiskUpdateSource extends RichSourceFunction<RiskUpdate> {
    private volatile boolean isRunning = true;
    private static final Random RANDOM = new Random();
    private static final String[] ASSET_TYPES = {"Stocks", "Bonds", "Commodities", "Real Estate", "Cryptocurrency"};

    @Override
    public void run(SourceContext<RiskUpdate> sourceContext) throws Exception {
        while (isRunning) {
            String assetType = ASSET_TYPES[RANDOM.nextInt(ASSET_TYPES.length)];
            double riskWeight = RANDOM.nextDouble();

            sourceContext.collectWithTimestamp(new RiskUpdate(assetType, riskWeight), System.currentTimeMillis());
            sourceContext.emitWatermark(new Watermark(System.currentTimeMillis() - 1));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}