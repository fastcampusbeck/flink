package com.fastcampus.streaming.flinkcourse.chapter3.basic.reduce.conversion;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ReducePracticeKeyedProcessFunction extends KeyedProcessFunction<String, StockTransaction, StockTransaction> {
    private transient ValueState<StockTransaction> maxVolumeTransactionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<StockTransaction> descriptor =
                new ValueStateDescriptor<>(
                        "maxVolumeTransaction", // the state name
                        TypeInformation.of(new TypeHint<>() {})); // type information
        maxVolumeTransactionState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(StockTransaction stockTransaction, KeyedProcessFunction<String, StockTransaction, StockTransaction>.Context context, Collector<StockTransaction> collector) throws Exception {
        StockTransaction currentMax = maxVolumeTransactionState.value();
        if (currentMax == null | stockTransaction.getVolume() > currentMax.getVolume()) {
            maxVolumeTransactionState.update(stockTransaction);
            collector.collect(stockTransaction);
        }
    }
}
