package com.fastcampus.streaming.flinkcourse.semi.operator.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.linalg.DenseVector;

public class CombineVectorsFunction implements ReduceFunction<Tuple5<DenseVector, String, Long, Integer, Integer>> {
    @Override
    public Tuple5<DenseVector, String, Long, Integer, Integer> reduce(Tuple5<DenseVector, String, Long, Integer, Integer> value1, Tuple5<DenseVector, String, Long, Integer, Integer> value2) {
        double[] combinedArray = new double[value1.f0.size()];
        for (int i = 0; i < value1.f0.size(); i++) {
            combinedArray[i] = value1.f0.get(i) + value2.f0.get(i);
        }
        int totalCount = value1.f4 + value2.f4;
        for (int i = 0; i < combinedArray.length; i++) {
            combinedArray[i] /= totalCount;
        }
        long latestBroadcastDate = (value1.f2 >= value2.f2 ? value1.f2 : value2.f2);
        return new Tuple5<>(new DenseVector(combinedArray), "", latestBroadcastDate, value1.f3, totalCount);
    }
}