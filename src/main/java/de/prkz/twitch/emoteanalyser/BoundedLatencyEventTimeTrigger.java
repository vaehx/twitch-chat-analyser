package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * This trigger is essentially an event-time trigger, but also starts a processing-time
 * timer with the first element in a pane. If the watermark did not pass the end of the
 * window before the processing-time timer fires, a "partial" window result can be
 * calculated to allow a fixed latency.
 */
public class BoundedLatencyEventTimeTrigger extends Trigger<Object, TimeWindow> {

    private final long maxLatency;

    private final ValueStateDescriptor<Long> timeoutTimeDescriptor =
            new ValueStateDescriptor<Long>("timeout-time", LongSerializer.INSTANCE);

    private BoundedLatencyEventTimeTrigger(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());

            // only register timeout timer for first element
            ValueState<Long> timeoutTimeState = ctx.getPartitionedState(timeoutTimeDescriptor);
            if (timeoutTimeState.value() == null) {
                long timeoutTime = ctx.getCurrentProcessingTime() + maxLatency;
                ctx.registerProcessingTimeTimer(timeoutTime);
                timeoutTimeState.update(timeoutTime);
            }

            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Long> timeoutTime = ctx.getPartitionedState(timeoutTimeDescriptor);
        if (timeoutTime.value() == time) {
            // early-fire pane, next timeout timer will be registered with next element
            timeoutTime.clear();
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            // clear timeout timer
            ValueState<Long> timeoutTime = ctx.getPartitionedState(timeoutTimeDescriptor);
            if (timeoutTime.value() != null) {
                ctx.deleteProcessingTimeTimer(timeoutTime.value());
                timeoutTime.clear();
            }

            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Long> timeoutTime = ctx.getPartitionedState(timeoutTimeDescriptor);
        if (timeoutTime.value() != null) {
            ctx.deleteProcessingTimeTimer(timeoutTime.value());
            timeoutTime.clear();
        }

        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public String toString() {
        return "BoundedLatencyEventTimeTrigger(maxLatency=" + maxLatency + "ms)";
    }

    public static BoundedLatencyEventTimeTrigger of(Time maxLatency) {
        return new BoundedLatencyEventTimeTrigger(maxLatency.toMilliseconds());
    }
}
