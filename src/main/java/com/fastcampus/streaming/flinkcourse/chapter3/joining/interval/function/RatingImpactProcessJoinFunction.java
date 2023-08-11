package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.function;


import com.fastcampus.streaming.flinkcourse.model.trade.RatingChange;
import com.fastcampus.streaming.flinkcourse.model.trade.Trade;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class RatingImpactProcessJoinFunction extends ProcessJoinFunction<Trade, RatingChange, String> {
    @Override
    public void processElement(Trade trade, RatingChange ratingChange, ProcessJoinFunction<Trade, RatingChange, String>.Context context, Collector<String> collector) throws Exception {
        collector.collect("Security ID: " + trade.getSecurityId()
                + ", Trade Price: " + trade.getTradePrice()
                + ", New Rating: " + ratingChange.getNewRating()
                + ", Timestamps: trade -> " + trade.getTimestamp()
                + ", rating change -> " + ratingChange.getTimestamp());
    }
}
