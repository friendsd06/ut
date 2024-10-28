package com.connectflow.dag;

public class ExecutionMetric {
    private final long duration;

    public ExecutionMetric(long duration) {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration cannot be negative");
        }
        this.duration = duration;
    }

    public long getDuration() {
        return duration;
    }
}
