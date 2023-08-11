package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.source;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithCustomer;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;

public class TradeWithCustomerSource extends RichSourceFunction<AssetTradeWithCustomer> {

    private volatile boolean isRunning = true;
    private static final Random RANDOM = new Random();

    private static final String[] ASSETS = {"Stocks", "Bonds", "Commodities", "Real Estate", "Cryptocurrency"};
    private static final String[] CUSTOMER_IDS = {"CUST1", "CUST2", "CUST3", "CUST4", "CUST5"};

    @Override
    public void run(SourceContext<AssetTradeWithCustomer> sourceContext) throws Exception {
        while (isRunning) {
            AssetTradeWithCustomer assetTradeWithCustomer = new AssetTradeWithCustomer();
            assetTradeWithCustomer.setCustomerId(CUSTOMER_IDS[RANDOM.nextInt(CUSTOMER_IDS.length)]);
            assetTradeWithCustomer.setAsset(ASSETS[RANDOM.nextInt(ASSETS.length)]);
            assetTradeWithCustomer.setAmount(1 + RANDOM.nextInt(100));
            assetTradeWithCustomer.setPrice(50 + RANDOM.nextDouble() * 150);
            assetTradeWithCustomer.setIsBuy(RANDOM.nextBoolean());

            sourceContext.collectWithTimestamp(assetTradeWithCustomer, System.currentTimeMillis());
            sourceContext.emitWatermark(new Watermark(System.currentTimeMillis() - 1));

            // Emit items at a rate of approximately one item per second
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}