package com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.source;

import com.fastcampus.streaming.flinkcourse.model.stock.StockPrediction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class StockPredictionSource  extends RichSourceFunction<StockPrediction> {
    private volatile boolean isRunning = true;
    private static final Random RANDOM = new Random();
    protected static final List<String> SYMBOLS = Arrays.asList("AAPL", "GOOGL", "MSFT", "AMZN", "FB");

    @Override
    public void run(SourceContext<StockPrediction> sourceContext) throws Exception {
        while (isRunning) {
            String symbol = SYMBOLS.get(RANDOM.nextInt(SYMBOLS.size()));
            double predictedPrice = RANDOM.nextDouble() * 1000; // random price between 0 and 1000
            long timestamp = System.currentTimeMillis();

            StockPrediction prediction = new StockPrediction(symbol, predictedPrice, timestamp);
            sourceContext.collect(prediction);

            Thread.sleep(1000); // sleep for 1 second before the next prediction
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}