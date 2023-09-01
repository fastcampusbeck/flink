package com.fastcampus.streaming.flinkcourse.semi.operator.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

public class FlattenStoredNewsFunction implements FlatMapFunction<List<Row>, Row> {
    @Override
    public void flatMap(List<Row> list, Collector<Row> out) {
        for (Row row : list) {
            out.collect(row);
        }
    }
}