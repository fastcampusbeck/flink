package com.fastcampus.streaming.flinkcourse.chapter3.connect.coprocess.function;

import com.fastcampus.streaming.flinkcourse.model.news.StockNews;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class SideOutputCoProcessFunction extends CoProcessFunction<StockTransaction, StockNews, String> {
    OutputTag<String> lowImportanceTag = new OutputTag<>("lowImportanceAlerts") {};
    OutputTag<String> mediumImportanceTag = new OutputTag<>("mediumImportanceAlerts") {};
    OutputTag<String> highImportanceTag = new OutputTag<>("highImportanceAlerts") {};

    private final Map<String, Map<String, String>> stockUpdates = new HashMap<>();

    @Override
    public void processElement1(StockTransaction stockTransaction, CoProcessFunction<StockTransaction, StockNews, String>.Context context, Collector<String> collector) throws Exception {
        Map<String, String> stockUpdate = stockUpdates.get(stockTransaction.getSymbol());
        if (stockUpdate != null) {
            for (Map.Entry<String, String> entry : stockUpdate.entrySet()) {
                String alert = "Alert for " + stockTransaction.getSymbol() + ": " + entry.getKey() + " - " + entry.getValue();
                switch (entry.getValue()) {
                    case "LOW":
                        context.output(lowImportanceTag, alert);
                        break;
                    case "MEDIUM":
                        context.output(mediumImportanceTag, alert);
                        break;
                    case "HIGH":
                        context.output(highImportanceTag, alert);
                        break;
                }
            }
        }
    }

    @Override
    public void processElement2(StockNews stockNews, CoProcessFunction<StockTransaction, StockNews, String>.Context context, Collector<String> collector) throws Exception {
        stockUpdates.put(stockNews.getSymbol(), stockNews.getUpdateInfo());
    }
}
