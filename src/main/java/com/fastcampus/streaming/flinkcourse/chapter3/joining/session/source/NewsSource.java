package com.fastcampus.streaming.flinkcourse.chapter3.joining.session.source;

import com.fastcampus.streaming.flinkcourse.model.news.News;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class NewsSource extends RichSourceFunction<News> {
        private volatile boolean isRunning = true;
        private final Random random = new Random();
        protected static final List<String> SYMBOLS = Arrays.asList("AAPL", "GOOGL", "MSFT", "AMZN", "FB");
        private static final HashMap<String, Boolean> TITLE_TO_POSITIVITY = new HashMap<>();
        static {
                TITLE_TO_POSITIVITY.put("Stock is going up!", true);
                TITLE_TO_POSITIVITY.put("New product launched!", true);
                TITLE_TO_POSITIVITY.put("Stock is going down!", false);
                TITLE_TO_POSITIVITY.put("Company scandal!", false);
                TITLE_TO_POSITIVITY.put("Company reports record profits!", true);
                TITLE_TO_POSITIVITY.put("CEO steps down.", false);
                TITLE_TO_POSITIVITY.put("Product recall announced.", false);
                TITLE_TO_POSITIVITY.put("Company enters new market.", true);
                TITLE_TO_POSITIVITY.put("Legal troubles for company.", false);
                TITLE_TO_POSITIVITY.put("Company patent approved.", true);
        }

        @Override
        public void run(SourceContext<News> ctx) throws Exception {
                String[] titles = TITLE_TO_POSITIVITY.keySet().toArray(new String[0]);

                while (isRunning) {
                        String symbol = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
                        String title = titles[random.nextInt(titles.length)];
                        boolean positivity = TITLE_TO_POSITIVITY.get(title);
                        long timestamp = System.currentTimeMillis();

                        ctx.collect(new News(symbol, title, positivity, timestamp));

                        Thread.sleep(500);
                }
        }

        @Override
        public void cancel() {
                isRunning = false;
        }
}