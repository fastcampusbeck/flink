package com.fastcampus.streaming.flinkcourse.semi.operator.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.linalg.DenseVector;

public class Tuple4ToTuple5Function implements MapFunction<Tuple4<DenseVector, String, Long, Integer>, Tuple5<DenseVector, String, Long, Integer, Integer>> {
    @Override
    public Tuple5<DenseVector, String, Long, Integer, Integer> map(Tuple4<DenseVector, String, Long, Integer> value) {
        return new Tuple5<>(value.f0, value.f1, value.f2, value.f3, 1);
    }
}