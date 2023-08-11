package com.fastcampus.streaming.flinkcourse.chapter3.connect.coflatmap.function;

import com.fastcampus.streaming.flinkcourse.model.recommendation.BotDirective;
import com.fastcampus.streaming.flinkcourse.model.recommendation.StockRecommendation;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class BotRecommendationRichCoFlatMapFunction  extends RichCoFlatMapFunction<StockTransaction, BotDirective, StockRecommendation> {
    private transient Map<String, BotDirective> activeDirectives;

    @Override
    public void open(Configuration parameters) throws Exception {
        activeDirectives = new HashMap<>();
    }
    @Override
    public void flatMap1(StockTransaction stockTransaction, Collector<StockRecommendation> collector) throws Exception {
        activeDirectives.values().stream()
                .filter(botDirective -> botDirective.getExpirationTimestamp() > stockTransaction.getTimestamp())
                .filter(botDirective -> stockTransaction.getPrice() >= botDirective.getMinPrice() && stockTransaction.getPrice() <= botDirective.getMaxPrice())
                .map(botDirective -> new StockRecommendation(stockTransaction.getSymbol(), botDirective.getDirectiveType(), stockTransaction.getPrice()))
                .forEach(collector::collect);
    }

    @Override
    public void flatMap2(BotDirective botDirective, Collector<StockRecommendation> collector) throws Exception {
        activeDirectives.compute(botDirective.getSymbol(),
                (k, v) -> v == null || botDirective.getTimestamp() > v.getTimestamp() ? botDirective : v);
    }
}
