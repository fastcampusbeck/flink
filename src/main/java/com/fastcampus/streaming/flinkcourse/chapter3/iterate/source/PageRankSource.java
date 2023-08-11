package com.fastcampus.streaming.flinkcourse.chapter3.iterate.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Map;

public class PageRankSource extends RichParallelSourceFunction<Map<String, Double>> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Map<String, Double>> sourceContext) throws Exception {
        Map<String, Double> ranks = new HashMap<>();
        ranks.put("AAPL", 1.0);
        ranks.put("MSFT", 1.0);
        ranks.put("GOOG", 1.0);
        ranks.put("AMZN", 1.0);
        ranks.put("FB", 1.0);

        sourceContext.collect(ranks);  // Emit initial rank values
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
