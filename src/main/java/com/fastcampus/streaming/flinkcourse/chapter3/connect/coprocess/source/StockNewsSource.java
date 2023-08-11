package com.fastcampus.streaming.flinkcourse.chapter3.connect.coprocess.source;

import com.fastcampus.streaming.flinkcourse.model.news.StockNews;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.*;

public class StockNewsSource extends RichSourceFunction<StockNews> {
    private volatile boolean isRunning = true;
    private static final Random random = new Random();
    private static final String[] SYMBOLS = {"AAPL", "GOOGL", "MSFT", "AMZN", "FB"};
    private static final Map<String, String> EVENT_IMPORTANCE;

    static {
        EVENT_IMPORTANCE = new HashMap<>();
        EVENT_IMPORTANCE.put("Earnings reports", "HIGH");
        EVENT_IMPORTANCE.put("Management changes", "MEDIUM");
        EVENT_IMPORTANCE.put("Regulatory news", "HIGH");
        EVENT_IMPORTANCE.put("Product news", "MEDIUM");
        EVENT_IMPORTANCE.put("Market news", "LOW");
    }


    @Override
    public void run(SourceContext<StockNews> sourceContext) throws Exception {
        List<String> eventTypes = new ArrayList<>(EVENT_IMPORTANCE.keySet());

        while (isRunning) {
            String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];  // Random stock symbol
            long timestamp = System.currentTimeMillis();

            Map<String, String> updateInfo = new HashMap<>();
            String randomEvent = eventTypes.get(random.nextInt(eventTypes.size()));
            String eventImportance = EVENT_IMPORTANCE.get(randomEvent);
            updateInfo.put(randomEvent, eventImportance);

            StockNews news = new StockNews();
            news.setSymbol(symbol);
            news.setTimestamp(timestamp);
            news.setUpdateInfo(updateInfo);
            sourceContext.collect(news);

            Thread.sleep(random.nextInt(5000));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
