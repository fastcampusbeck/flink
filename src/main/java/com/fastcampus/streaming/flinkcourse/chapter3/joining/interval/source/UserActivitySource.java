package com.fastcampus.streaming.flinkcourse.chapter3.joining.interval.source;

import com.fastcampus.streaming.flinkcourse.model.user.*;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.HashMap;
import java.util.Random;

public class UserActivitySource  extends RichParallelSourceFunction<UserEvent> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final HashMap<String, Long> userLastActivityTime = new HashMap<>();

    private final HashMap<String, String> userToSessionId = new HashMap<>();  // keeps track of active session for each user

    @Override
    public void run(SourceContext<UserEvent> sourceContext) throws Exception {
        while (isRunning) {
            String userId = "user" + random.nextInt(10);
            long currentTime = System.currentTimeMillis();


            // If this user is not active or a session timeout occurred, generate a new sessionId
            if (!userLastActivityTime.containsKey(userId) || (currentTime - userLastActivityTime.get(userId) > 10000)) {
                String sessionId = "session_" + userId + "_" + currentTime;
                userToSessionId.put(userId, sessionId);

                LoginEvent loginEvent = new LoginEvent(userId, sessionId, currentTime);
                sourceContext.collect(loginEvent);
                userLastActivityTime.put(userId, currentTime);

                Thread.sleep(random.nextInt(500) + 500);
            }

            // Generate a ViewEvent
            String stockId = "stock" + random.nextInt(100);
            ViewEvent viewEvent = new ViewEvent(userId, userToSessionId.get(userId), currentTime, stockId);
            sourceContext.collect(viewEvent);
            userLastActivityTime.put(userId, currentTime);
            Thread.sleep(random.nextInt(500) + 500);

            // Generate an AnalysisEvent
            AnalysisEvent analysisEvent = new AnalysisEvent(userId, userToSessionId.get(userId), currentTime, "analysis" + random.nextInt(10));
            sourceContext.collect(analysisEvent);
            userLastActivityTime.put(userId, currentTime);
            Thread.sleep(random.nextInt(500) + 500);

            // Generate a TradeEvent if the analysis result is higher than a certain threshold
            if (random.nextDouble() > 0.5) {
                TradeEvent tradeEvent = new TradeEvent(userId, userToSessionId.get(userId), currentTime, stockId, random.nextDouble() * 100);
                sourceContext.collect(tradeEvent);
                userLastActivityTime.put(userId, currentTime);
                Thread.sleep(random.nextInt(500) + 500);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
