package com.fastcampus.streaming.flinkcourse.chapter3.basic.keyby.conversion;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockWithCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyByPracticeKeyedProcessFunction  extends KeyedProcessFunction<String, Stock, StockWithCount> {
    private transient ValueState<Long> symbolCountState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("symbolCount", // the state name
                        TypeInformation.of(new TypeHint<>() {})); // type information
        symbolCountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Stock stock, KeyedProcessFunction<String, Stock, StockWithCount>.Context context, Collector<StockWithCount> collector) throws Exception {
        if (symbolCountState.value() == null) {
            symbolCountState.update(0L);
        }

        long newCount = symbolCountState.value() + 1;
        symbolCountState.update(newCount);
        collector.collect(new StockWithCount(stock.getSymbol(), stock.getPrice(), stock.getTimestamp(), newCount));
    }
}
