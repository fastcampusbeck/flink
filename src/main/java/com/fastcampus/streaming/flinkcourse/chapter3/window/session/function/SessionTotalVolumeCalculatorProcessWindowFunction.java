package com.fastcampus.streaming.flinkcourse.chapter3.window.session.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SessionTotalVolumeCalculatorProcessWindowFunction extends ProcessWindowFunction<StockTransaction, Tuple5<String, Long, Long, Long, Long>, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<StockTransaction, Tuple5<String, Long, Long, Long, Long>, String, TimeWindow>.Context context, Iterable<StockTransaction> iterable, Collector<Tuple5<String, Long, Long, Long, Long>> collector) throws Exception {
        long sum = 0;
        for (StockTransaction stockTransaction : iterable) {
            sum += stockTransaction.getVolume();
        }
        collector.collect(new Tuple5<>(s, context.window().getStart(), context.window().getEnd(), context.window().maxTimestamp(), sum));
    }
}
