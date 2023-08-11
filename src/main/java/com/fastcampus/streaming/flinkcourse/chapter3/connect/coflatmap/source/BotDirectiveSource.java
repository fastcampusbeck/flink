package com.fastcampus.streaming.flinkcourse.chapter3.connect.coflatmap.source;

import com.fastcampus.streaming.flinkcourse.model.recommendation.BotDirective;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;
import java.util.UUID;

public class BotDirectiveSource extends RichParallelSourceFunction<BotDirective> {
    private volatile boolean isRunning;
    private final Random random = new Random();
    private static final String[] SYMBOLS = {"AAPL", "GOOGL", "MSFT", "AMZN", "FB"};
    private static final String[] DIRECTIVE_TYPES = {"BUY", "SELL", "HOLD"};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning = true;
    }

    @Override
    public void run(SourceContext<BotDirective> sourceContext) throws Exception {
        while (isRunning) {
            long currentTimestamp = System.currentTimeMillis();
            for (String symbol : SYMBOLS) {
                String directiveType = DIRECTIVE_TYPES[random.nextInt(DIRECTIVE_TYPES.length)];
                double minPrice = 100 + random.nextDouble() * 1000; // random price between 100 and 1100
                double maxPrice = minPrice + random.nextDouble() * 200; // maxPrice is minPrice plus up to 200
                long expirationTimestamp = currentTimestamp + random.nextInt(10000); // expires in up to 10 seconds

                BotDirective directive = new BotDirective(
                        UUID.randomUUID().toString(),
                        directiveType,
                        symbol,
                        minPrice,
                        maxPrice,
                        currentTimestamp,
                        expirationTimestamp
                );

                sourceContext.collectWithTimestamp(directive, currentTimestamp);
                sourceContext.emitWatermark(new Watermark(currentTimestamp - 1)); // assume events are 1 millisecond late

                // sleep for a random time before the next directive
                Thread.sleep(random.nextInt(500));
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
