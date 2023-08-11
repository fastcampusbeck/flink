package com.fastcampus.streaming.flinkcourse.chapter3.iterate.source;

import com.fastcampus.streaming.flinkcourse.model.stock.StockCorrelation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.*;

public class StockCorrelationSource extends RichSourceFunction<StockCorrelation> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    private static final Map<String, List<String>> STOCK_CORRELATIONS = new HashMap<>();
    static {
        STOCK_CORRELATIONS.put("AAPL", Arrays.asList("MSFT", "AMZN"));
        STOCK_CORRELATIONS.put("GOOGL", List.of("FB"));
        STOCK_CORRELATIONS.put("MSFT", Arrays.asList("AAPL", "GOOGL"));
        STOCK_CORRELATIONS.put("AMZN", List.of("AAPL"));
        STOCK_CORRELATIONS.put("FB", List.of("GOOGL"));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        isRunning = true;
    }

    @Override
    public void run(SourceContext<StockCorrelation> sourceContext) throws Exception {
        int stockIndex = 0;
        int correlationIndex = 0;

        List<String> stockSymbols = new ArrayList<>(STOCK_CORRELATIONS.keySet());

        while (isRunning) {
            String stockA = stockSymbols.get(random.nextInt(stockSymbols.size()));
            List<String> correlatedStocks = STOCK_CORRELATIONS.get(stockA);

            if (!correlatedStocks.isEmpty()) {
                String stockB = correlatedStocks.get(correlationIndex % correlatedStocks.size());
                double correlation = random.nextDouble();

                sourceContext.collect(new StockCorrelation(stockA, stockB, correlation));
                correlationIndex++;
                if (correlationIndex >= correlatedStocks.size()) {
                    correlationIndex = 0;
                    stockIndex = (stockIndex + 1) % stockSymbols.size();
                }
            }

            // Sleep for 1 second before generating next
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
