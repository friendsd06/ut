package com.connectflow.dag.execution;

import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.metrics.ExecutionMetrics;

import java.util.Map;

public class DAGExecutionResult<T, R> {
    private final long startTime;
    private final long endTime;
    private final Map<T, ExecutionResult<T, R>> nodeResults;
    private final ExecutionMetrics executionMetrics;

    private DAGExecutionResult(Builder<T, R> builder) {
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.nodeResults = builder.nodeResults;
        this.executionMetrics = builder.executionMetrics;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Map<T, ExecutionResult<T, R>> getNodeResults() {
        return nodeResults;
    }

    public ExecutionMetrics getExecutionMetrics() {
        return executionMetrics;
    }

    public long getExecutionTime() {
        return endTime - startTime;
    }

    public static class Builder<T, R> {
        private long startTime;
        private long endTime;
        private Map<T, ExecutionResult<T, R>> nodeResults;
        private ExecutionMetrics executionMetrics;

        public Builder<T, R> setStartTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder<T, R> setEndTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder<T, R> setNodeResults(Map<T, ExecutionResult<T, R>> nodeResults) {
            this.nodeResults = nodeResults;
            return this;
        }

        public Builder<T, R> setExecutionMetrics(ExecutionMetrics executionMetrics) {
            this.executionMetrics = executionMetrics;
            return this;
        }

        public DAGExecutionResult<T, R> build() {
            return new DAGExecutionResult<>(this);
        }
    }
}
