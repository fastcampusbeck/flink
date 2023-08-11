package com.fastcampus.streaming.flinkcourse.chapter4.valuestate.function;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CumulativeVolumeKeyedProcessFunction extends KeyedProcessFunction<String, StockTransaction, Tuple2<String, Long>> {
    private transient ValueState<Long> cumulativeVolumeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "cumulativeVolume", TypeInformation.of(new TypeHint<>() {}));
        cumulativeVolumeState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(StockTransaction stockTransaction, KeyedProcessFunction<String, StockTransaction, Tuple2<String, Long>>.Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
        if (cumulativeVolumeState.value() == null) {
            cumulativeVolumeState.update(0L);
        }

        Long cumulativeVolume = cumulativeVolumeState.value();
        cumulativeVolume += stockTransaction.getVolume();
        cumulativeVolumeState.update(cumulativeVolume);
        collector.collect(new Tuple2<>(stockTransaction.getSymbol(), cumulativeVolume));
    }
}
