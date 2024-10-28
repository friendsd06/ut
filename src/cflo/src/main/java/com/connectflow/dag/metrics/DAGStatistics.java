package com.connectflow.dag.metrics;

import com.connectflow.dag.DAGNode;

import java.util.List;

public class DAGStatistics<T, R> {
    private final int nodeCount;
    private final int edgeCount;
    private final int maxDepth;
    private final List<DAGNode<T, R>> criticalPath;
    private final double parallelizationPotential;

    public DAGStatistics(int nodeCount, int edgeCount, int maxDepth,
                         List<DAGNode<T, R>> criticalPath, double parallelizationPotential) {
        this.nodeCount = nodeCount;
        this.edgeCount = edgeCount;
        this.maxDepth = maxDepth;
        this.criticalPath = criticalPath;
        this.parallelizationPotential = parallelizationPotential;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getEdgeCount() {
        return edgeCount;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public List<DAGNode<T, R>> getCriticalPath() {
        return criticalPath;
    }

    public double getParallelizationPotential() {
        return parallelizationPotential;
    }
}
