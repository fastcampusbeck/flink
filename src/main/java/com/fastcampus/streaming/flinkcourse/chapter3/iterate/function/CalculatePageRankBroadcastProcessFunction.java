package com.fastcampus.streaming.flinkcourse.chapter3.iterate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockCorrelation;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fastcampus.streaming.flinkcourse.chapter3.iterate.iteratePractice.correlationsDescriptor;

public class CalculatePageRankBroadcastProcessFunction  extends BroadcastProcessFunction<Map<String, Double>, StockCorrelation, Map<String, Double>> {
    private static final double damping = 0.85;

    @Override
    public void processElement(Map<String, Double> pageRanks, ReadOnlyContext ctx, Collector<Map<String, Double>> out) throws Exception {
        Map<String, Double> newPageRanks = new HashMap<>();
        for (String stock : pageRanks.keySet()) {
            double sum = 0;
            // Get the broadcasted correlations
            List<StockCorrelation> stockCorrelations = ctx.getBroadcastState(correlationsDescriptor).get(stock);

            if (stockCorrelations != null) {
                for (StockCorrelation sc : stockCorrelations) {
                    sum += pageRanks.getOrDefault(sc.getStockB(), 0.0) * sc.getCorrelation();
                }
            }
            double newPageRank = (1 - damping) + damping * sum;
            newPageRanks.put(stock, newPageRank);
        }

        out.collect(newPageRanks);  // Emit the new PageRank values
    }

    @Override
    public void processBroadcastElement(StockCorrelation correlation, Context ctx, Collector<Map<String, Double>> out) throws Exception {
        BroadcastState<String, List<StockCorrelation>> correlationsState = ctx.getBroadcastState(correlationsDescriptor);

        // Update correlations state
        List<StockCorrelation> existing = correlationsState.get(correlation.getStockA());
        if (existing == null) {
            existing = new ArrayList<>();
        }
        existing.add(correlation);
        correlationsState.put(correlation.getStockA(), existing);
    }
}