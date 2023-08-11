package com.fastcampus.streaming.flinkcourse.chapter2.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // normalize and split the line into words
        String[] words = value.toLowerCase().split("\\W+");

        // emit the words
        for (String word : words) {
            if (word.length() > 0) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
