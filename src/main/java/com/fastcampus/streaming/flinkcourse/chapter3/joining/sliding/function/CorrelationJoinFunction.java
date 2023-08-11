package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.function;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CorrelationJoinFunction implements JoinFunction<Tuple2<String, Tuple2<Double, Double>>, Tuple2<String, Tuple2<Double, Double>>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> join(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2, Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple22) throws Exception {
        double correlation = getCorrelation(stringTuple2Tuple2, stringTuple2Tuple22);
        return new Tuple2<>(stringTuple2Tuple2.f0, correlation);
    }

    private static double getCorrelation(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2, Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple22) {
        Tuple2<Double, Double> firstTuple2 = stringTuple2Tuple2.f1;
        Tuple2<Double, Double> secondTuple2 = stringTuple2Tuple22.f1;

        double mean1 = firstTuple2.f0 / firstTuple2.f1;
        double mean2 = secondTuple2.f0 / secondTuple2.f1;

        double variance1 = firstTuple2.f0 / firstTuple2.f1 - mean1 * mean1;
        double variance2 = secondTuple2.f0 / secondTuple2.f1 - mean2 * mean2;

        double covariance = firstTuple2.f0 / firstTuple2.f1 - mean1 * mean2;

        return covariance / Math.sqrt(variance1 * variance2);
    }
}
