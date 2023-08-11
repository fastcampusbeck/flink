package com.fastcampus.streaming.flinkcourse.chapter4.valuestate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class TradingAnalysisKeyedProcessFunction extends KeyedProcessFunction<String, StockTransaction, String> {
    private transient ValueState<Double> totalTransactionValueState;
    private transient ValueState<Long> totalNumberOfTradesState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> totalValueDescriptor = new ValueStateDescriptor<>(
                "totalTransactionValue", TypeInformation.of(new TypeHint<>() {}));

        ValueStateDescriptor<Long> totalTradesDescriptor = new ValueStateDescriptor<>(
                "totalNumberOfTrades",
                TypeInformation.of(new TypeHint<>() {}));

        totalTransactionValueState = getRuntimeContext().getState(totalValueDescriptor);
        totalNumberOfTradesState = getRuntimeContext().getState(totalTradesDescriptor);
    }

    @Override
    public void processElement(StockTransaction stockTransaction, KeyedProcessFunction<String, StockTransaction, String>.Context context, Collector<String> collector) throws Exception {
        double totalTransactionValue = Optional.ofNullable(totalTransactionValueState.value()).orElse(0.0);
        totalTransactionValueState.update(totalTransactionValue);

        long totalNumberOfTrades = Optional.ofNullable(totalNumberOfTradesState.value()).orElse(0L);
        totalNumberOfTradesState.update(totalNumberOfTrades);

        totalTransactionValue += stockTransaction.getPrice() * stockTransaction.getVolume();
        totalNumberOfTrades += 1;

        totalTransactionValueState.update(totalTransactionValue);
        totalNumberOfTradesState.update(totalNumberOfTrades);

        double averageTransactionValue = totalTransactionValue / totalNumberOfTrades;

        // Generate alert if average transaction value exceeds a threshold
        if (averageTransactionValue > 80000) {
            collector.collect("Alert: Stock " + stockTransaction.getSymbol() + " has an average transaction value exceeding 80000");
        }

        collector.collect("Stock: " + stockTransaction.getSymbol() + ", Average Transaction Value: " + averageTransactionValue);
    }
}
