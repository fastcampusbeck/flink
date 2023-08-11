package com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source;

import com.fastcampus.streaming.flinkcourse.model.trade.EconomicIndicator;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public class EconomicIndicatorSourceFunction extends RichSourceFunction<EconomicIndicator> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private static final String[] INDICATORS = {"GDP", "CPI", "Unemployment Rate"};


    @Override
    public void run(SourceContext<EconomicIndicator> sourceContext) throws Exception {
        while (isRunning) {
            EconomicIndicator economicIndicator = new EconomicIndicator();
            String indicatorName = INDICATORS[random.nextInt(INDICATORS.length)];

            economicIndicator.setName(indicatorName);
            economicIndicator.setTimestamp(System.currentTimeMillis());
            economicIndicator.setValue(generateValue(indicatorName));

            sourceContext.collect(economicIndicator);

            Thread.sleep(1000);
        }
    }

    private double generateValue(String indicatorName) {
        switch (indicatorName) {
            case "GDP":
                return 50000 + random.nextDouble() * 10000; // Assume GDP per capita ranges from 50,000 to 60,000
            case "CPI":
                return 100 + random.nextDouble() * 10; // Assume CPI ranges from 100 to 110
            case "Unemployment Rate":
                return 3 + random.nextDouble() * 7; // Assume unemployment rate ranges from 3% to 10%
            default:
                throw new IllegalArgumentException("Unknown economic indicator: " + indicatorName);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
