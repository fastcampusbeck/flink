package com.fastcampus.streaming.flinkcourse.chapter3.window.global.trigger;

import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class PortfolioTrigger extends Trigger<Portfolio, GlobalWindow> {
    @Override
    public TriggerResult onElement(Portfolio portfolio, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        if (portfolio.isValid(triggerContext.getCurrentWatermark())) {
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

    }
}
