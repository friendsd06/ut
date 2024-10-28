package com.connectflow.dag.metrics;

import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionMetric;
import com.connectflow.dag.ExecutionResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DAGProgressTracker<T, R> {
    private final Map<T, ExecutionMetric> nodeMetrics;
    private final long startTime;

    public DAGProgressTracker() {
        this.nodeMetrics = new ConcurrentHashMap<>();
        this.startTime = System.currentTimeMillis();
    }

    public void updateProgress(DAGNode<T, R> node, ExecutionResult<T, R> result) {
        if (node == null || result == null) {
            throw new IllegalArgumentException("Node and result cannot be null");
        }
        nodeMetrics.put(node.getId(), new ExecutionMetric(result.getDuration()));
    }

    public ExecutionMetrics getMetrics() {
        long totalTime = System.currentTimeMillis() - startTime;
        return new ExecutionMetrics(nodeMetrics, totalTime);
    }
}
