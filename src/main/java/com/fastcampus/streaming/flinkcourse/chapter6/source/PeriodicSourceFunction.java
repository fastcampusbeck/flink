package com.fastcampus.streaming.flinkcourse.chapter6.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.List;

/** A source function that collects provided data periodically at a fixed interval. */
public class PeriodicSourceFunction implements SourceFunction<Row> {
    private final long interval;

    private final List<List<Row>> data;

    private int index = 0;

    private boolean isRunning = true;

    /**
     * @param interval The time interval in milliseconds to collect data into sourceContext.
     * @param data The data to be collected. Each element is a list of records to be collected
     *     between two adjacent intervals.
     */
    public PeriodicSourceFunction(long interval, List<List<Row>> data) {
        this.interval = interval;
        this.data = data;
    }

    @Override
    public void run(SourceFunction.SourceContext<Row> sourceContext) throws Exception {
        while (isRunning) {
            for (Row data : this.data.get(index)) {
                sourceContext.collect(data);
            }
            Thread.sleep(interval);
            index = (index + 1) % this.data.size();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}