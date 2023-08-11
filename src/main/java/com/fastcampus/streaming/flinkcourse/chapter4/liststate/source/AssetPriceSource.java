package com.fastcampus.streaming.flinkcourse.chapter4.liststate.source;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetPrice;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public class AssetPriceSource extends RichSourceFunction<AssetPrice> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private static final String[] ASSET_NAMES = {"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"};
    private static final String[] PORTFOLIO_IDS = {"Portfolio1", "Portfolio2", "Portfolio3"};

    @Override
    public void run(SourceContext<AssetPrice> sourceContext) throws Exception {
        while (isRunning) {
            AssetPrice assetPrice = new AssetPrice();
            assetPrice.setAssetName(ASSET_NAMES[random.nextInt(ASSET_NAMES.length)]);
            assetPrice.setPrice(random.nextDouble() * 1000);
            assetPrice.setPortfolioId(PORTFOLIO_IDS[random.nextInt(PORTFOLIO_IDS.length)]);
            assetPrice.setTimestamp(System.currentTimeMillis());
            sourceContext.collect(assetPrice);

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
