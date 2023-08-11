package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.source;

import com.fastcampus.streaming.flinkcourse.model.trade.RatingChange;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public class RatingChangeEventSource extends RichSourceFunction<RatingChange> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    private static final String[] SECURITIES = {"sec1", "sec2", "sec3", "sec4", "sec5"};
    private static final String[] POSSIBLE_RATINGS = {"AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "D"};

    @Override
    public void run(SourceContext<RatingChange> sourceContext) throws Exception {
        while (isRunning) {
            RatingChange ratingChangeEvent = new RatingChange();
            ratingChangeEvent.setSecurityId(SECURITIES[random.nextInt(SECURITIES.length)]);
            ratingChangeEvent.setNewRating(POSSIBLE_RATINGS[random.nextInt(POSSIBLE_RATINGS.length)]);
            ratingChangeEvent.setTimestamp(System.currentTimeMillis());

            sourceContext.collect(ratingChangeEvent);

            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
