package com.fastcampus.streaming.flinkcourse.semi.operator.flatmap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class NewsDataStateStoringFunction extends RichFlatMapFunction<Row, List<Row>> {

    private transient ListState<Row> newsDataState;
    private static final int MAX_SIZE = 10000;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ListStateDescriptor<Row> descriptor =
                new ListStateDescriptor<>("newsData", TypeInformation.of(new TypeHint<>() {}));
        newsDataState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Row value, Collector<List<Row>> out) throws Exception {
        List<Row> storedNewsData = new ArrayList<>();

        newsDataState.add(value);

        for (Row data : newsDataState.get()) {
            storedNewsData.add(data);
        }

        while (storedNewsData.size() > MAX_SIZE) {
            storedNewsData.remove(0);
        }

        newsDataState.clear();
        for (Row data : storedNewsData) {
            newsDataState.add(data);
        }

        out.collect(storedNewsData);
    }
}

