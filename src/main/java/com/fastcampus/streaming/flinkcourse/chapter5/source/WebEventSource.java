package com.fastcampus.streaming.flinkcourse.chapter5.source;

import com.fastcampus.streaming.flinkcourse.model.cep.WebEvent;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class WebEventSource extends RichSourceFunction<WebEvent> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<WebEvent> ctx) throws Exception {
        int count = 0;

        while (isRunning) {
            // 5번마다 패턴을 연속적으로 발생시키기 위한 로직
            if (count % 5 == 0) {
                // 연속적으로 6번의 LOGIN_FAILURE 이벤트 발생 (패턴이 두 번 연속 감지될 수 있음)
                for (int i = 0; i < 6; i++) {
                    ctx.collect(new WebEvent("LOGIN_FAILURE"));
                }
                // LOGIN_SUCCESS 및 UNUSUAL_ACTIVITY 이벤트 발생
                ctx.collect(new WebEvent("LOGIN_SUCCESS"));
                ctx.collect(new WebEvent("UNUSUAL_ACTIVITY"));
            } else {
                // 다른 경우에는 일반적인 이벤트 순서대로 발생
                ctx.collect(new WebEvent("LOGIN_FAILURE"));
                ctx.collect(new WebEvent("LOGIN_SUCCESS"));
                ctx.collect(new WebEvent("UNUSUAL_ACTIVITY"));
            }

            count++;
            Thread.sleep(1000);  // generate data every second
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}