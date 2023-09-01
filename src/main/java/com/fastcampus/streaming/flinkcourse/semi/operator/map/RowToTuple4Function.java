package com.fastcampus.streaming.flinkcourse.semi.operator.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.types.Row;

public class RowToTuple4Function implements MapFunction<Row, Tuple4<DenseVector, String, Long, Integer>> {
    @Override
    public Tuple4<DenseVector, String, Long, Integer> map(Row row) throws Exception {
        return new Tuple4<>(
                (DenseVector)row.getField(0), (String)row.getField(1), (Long)row.getField(2), (Integer)row.getField(3)
        );
    }
}
