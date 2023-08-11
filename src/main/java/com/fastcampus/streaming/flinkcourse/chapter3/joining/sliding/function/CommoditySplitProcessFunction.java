package com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.function;

import com.fastcampus.streaming.flinkcourse.model.commodity.Commodity;
import com.fastcampus.streaming.flinkcourse.model.commodity.GoldCommodity;
import com.fastcampus.streaming.flinkcourse.model.commodity.SilverCommodity;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import static com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.SlidingWindowJoinExercise.goldOutputTag;
import static com.fastcampus.streaming.flinkcourse.chapter3.joining.sliding.SlidingWindowJoinExercise.silverOutputTag;


public class CommoditySplitProcessFunction extends ProcessFunction<Commodity, Void> {
    @Override
    public void processElement(Commodity commodity, ProcessFunction<Commodity, Void>.Context context, Collector<Void> collector) throws Exception {
        if (commodity instanceof GoldCommodity) {
            context.output(goldOutputTag, (GoldCommodity) commodity);
        } else if (commodity instanceof SilverCommodity){
            context.output(silverOutputTag, (SilverCommodity) commodity);
        }
    }
}
