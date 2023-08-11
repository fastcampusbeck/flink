package com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.function;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import com.fastcampus.streaming.flinkcourse.model.stock.StockPrediction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

public class PriceComparisonRichCoMapFunction  extends RichCoMapFunction<Stock, StockPrediction, String> {
    private ValueState<Tuple2<Double, Long>> lastPredictedPrice;

    @Override
    public void open(Configuration parameters) throws Exception{
        ValueStateDescriptor<Tuple2<Double, Long>> descriptor = new ValueStateDescriptor<>(
                "lastPredictedPrice", // the state name
                TypeInformation.of(new TypeHint<>() {}));
        lastPredictedPrice = getRuntimeContext().getState(descriptor);
    }

    @Override
    public String map1(Stock stock) throws Exception {
        Tuple2<Double, Long> lastPrediction = lastPredictedPrice.value();

        if (lastPrediction != null && lastPrediction.f1 <= stock.getTimestamp()) {
            double predictedPrice = lastPrediction.f0;
            lastPredictedPrice.clear();
            return "Symbol: " + stock.getSymbol() + ", Price: " + stock.getPrice() + ", PredictedPrice: " + predictedPrice
                    + ", Accuracy: " + ((predictedPrice - stock.getPrice()) / stock.getPrice() * 100) + "%"
                    + ", Timestamp: " + stock.getTimestamp() + ", Prediction Timestamp: " + lastPrediction.f1;
        } else {
            return "Symbol: " + stock.getSymbol() + ", Price: " + stock.getPrice() + ", No predicted price available";
        }
    }

    @Override
    public String map2(StockPrediction stockPrediction) throws Exception {
        Tuple2<Double, Long> lastPrediction = lastPredictedPrice.value();

        if (lastPrediction == null || lastPrediction.f1 < stockPrediction.getPredictedPrice()) {
            lastPredictedPrice.update(Tuple2.of(stockPrediction.getPredictedPrice(), stockPrediction.getTimestamp()));
        }

        return "Symbol: " + stockPrediction.getSymbol() + ", PredictedPrice: " + stockPrediction.getPredictedPrice()
                + ", Prediction Timestamp: " + lastPredictedPrice.value().f1;
    }
}