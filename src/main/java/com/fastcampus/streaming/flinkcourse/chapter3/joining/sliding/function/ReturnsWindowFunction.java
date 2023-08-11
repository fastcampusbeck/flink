package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.function;

import com.fastcampus.streaming.flinkcourse.model.commodity.Commodity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReturnsWindowFunction<T extends Commodity> implements WindowFunction<T, Tuple2<String, Tuple2<Double, Double>>, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<T> iterable, Collector<Tuple2<String, Tuple2<Double, Double>>> collector) throws Exception {
        List<Commodity> sortedPrices = StreamSupport.stream(iterable.spliterator(), false)
                .sorted(Comparator.comparing(Commodity::getTimestamp))
                .collect(Collectors.toList());

        if (sortedPrices.size() > 1) {

            String exchange = sortedPrices.get(0).getExchange();

            double startPrice = sortedPrices.get(0).getPrice();
            double endPrice = sortedPrices.get(sortedPrices.size() - 1).getPrice();

            int count = sortedPrices.size();

            double returnVal = (endPrice / startPrice - 1) * count;

            collector.collect(new Tuple2<>(exchange, new Tuple2<>(returnVal, (double) count)));
        }
    }
}
