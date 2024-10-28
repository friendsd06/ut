package com.connectflow.dag.metrics;

import com.connectflow.dag.ExecutionMetric;

import java.util.Map;

public class ExecutionMetrics {
    private final Map<?, ExecutionMetric> metrics;
    private final long totalTime;

    public ExecutionMetrics(Map<?, ExecutionMetric> metrics, long totalTime) {
        if (metrics == null) {
            throw new IllegalArgumentException("Metrics cannot be null");
        }
        this.metrics = metrics;
        this.totalTime = totalTime;
    }

    public Map<?, ExecutionMetric> getMetrics() {
        return metrics;
    }

    public long getTotalTime() {
        return totalTime;
    }
}
