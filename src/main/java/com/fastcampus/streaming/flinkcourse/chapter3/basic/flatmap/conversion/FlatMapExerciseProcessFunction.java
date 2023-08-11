package com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap.conversion;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class FlatMapExerciseProcessFunction extends ProcessFunction<String, Stock> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void processElement(String s, ProcessFunction<String, Stock>.Context context, Collector<Stock> collector) throws Exception {
        List<Stock> stocks = objectMapper.readValue(s, new TypeReference<>() {});
        for (Stock stock : stocks) {
            collector.collect(stock);
        }
    }
}
