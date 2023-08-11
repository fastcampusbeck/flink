package com.fastcampus.streaming.flinkcourse.chapter3.window.global.conversion;

import com.fastcampus.streaming.flinkcourse.model.aggregate.TotalTradedVolume;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class GlobalWindowExerciseKeyedProcessFunction extends KeyedProcessFunction<String, StockTransaction, TotalTradedVolume> {
    private transient ValueState<Double> totalTransactionAmountState;
    private transient ValueState<Integer> transactionCountState;


    @Override
    public void open(Configuration parameters) throws Exception {
        totalTransactionAmountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalTransactionAmount", TypeInformation.of(new TypeHint<>() {})));
        transactionCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("transactionCount", TypeInformation.of(new TypeHint<>() {})));
    }

    @Override
    public void processElement(StockTransaction stockTransaction, KeyedProcessFunction<String, StockTransaction, TotalTradedVolume>.Context context, Collector<TotalTradedVolume> collector) throws Exception {
        double totalTransactionAmount = totalTransactionAmountState.value() == null ? 0 : totalTransactionAmountState.value();
        totalTransactionAmount += stockTransaction.getPrice() * stockTransaction.getVolume();
        totalTransactionAmountState.update(totalTransactionAmount);

        int transactionCount = transactionCountState.value() == null ? 0 : transactionCountState.value();
        transactionCount++;
        transactionCountState.update(transactionCount);

        if (transactionCount >= 10) {
            collector.collect(new TotalTradedVolume(stockTransaction.getSymbol(), totalTransactionAmount));
            totalTransactionAmountState.clear();
            transactionCountState.clear();
        }
    }
}
