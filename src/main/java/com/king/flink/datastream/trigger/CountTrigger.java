package com.king.flink.datastream.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CountTrigger extends Trigger<Object, TimeWindow> {

    private static int count = 1;

    private CountTrigger() {
    };

    @Override
    public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.registerProcessingTimeTimer(timeWindow.maxTimestamp());
        System.out.println("#######" + o);
        if (count > 9) {
            count = 1;
            return TriggerResult.FIRE_AND_PURGE;
        }else{
            count++;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteProcessingTimeTimer(timeWindow.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
    }

    public static CountTrigger create() {
        return new CountTrigger();
    }
}
