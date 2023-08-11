package com.fastcampus.streaming.flinkcourse.chapter3.basic.flatmap.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.List;

public class StockFlatMapFunction implements FlatMapFunction<String, Stock> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(String s, Collector<Stock> collector) throws Exception {
       List<Stock> stocks = objectMapper.readValue(s, new TypeReference<>() {});
       for (Stock stock: stocks) {
           collector.collect(stock);
       }
    }
}
