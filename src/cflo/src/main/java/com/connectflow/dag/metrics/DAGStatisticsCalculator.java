package com.connectflow.dag.metrics;

import com.connectflow.dag.DAG;
import com.connectflow.dag.DAGNode;
import com.connectflow.dag.util.ExecutionPathFinder;

import java.util.*;
import java.util.stream.Collectors;

public class DAGStatisticsCalculator<T, R> {

    public DAGStatistics<T, R> calculateStatistics(DAG<T, R> dag) {
        int nodeCount = dag.getNodes().size();
        int edgeCount = dag.getNodes().stream()
                .mapToInt(node -> node.getOutgoingNodes().size())
                .sum();
        int maxDepth = calculateMaxDepth(dag);
        List<DAGNode<T, R>> criticalPath = calculateCriticalPath(dag);
        double parallelizationPotential = calculateParallelizationPotential(dag, criticalPath);

        return new DAGStatistics<>(nodeCount, edgeCount, maxDepth, criticalPath, parallelizationPotential);
    }

    private int calculateMaxDepth(DAG<T, R> dag) {
        Map<DAGNode<T, R>, Integer> depths = new HashMap<>();
        for (DAGNode<T, R> root : dag.getRootNodes()) {
            calculateNodeDepth(root, depths);
        }
        return depths.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    }

    private int calculateNodeDepth(DAGNode<T, R> node, Map<DAGNode<T, R>, Integer> depths) {
        if (depths.containsKey(node)) {
            return depths.get(node);
        }
        int maxDepth = node.getIncomingNodes().stream()
                .mapToInt(parent -> calculateNodeDepth(parent, depths))
                .max()
                .orElse(-1);
        int depth = maxDepth + 1;
        depths.put(node, depth);
        return depth;
    }

    private List<DAGNode<T, R>> calculateCriticalPath(DAG<T, R> dag) {
        Map<DAGNode<T, R>, Long> nodeWeights = dag.getNodes().stream()
                .collect(Collectors.toMap(
                        node -> node,
                        node -> node.getEstimatedDuration().toMillis()
                ));
        return new ExecutionPathFinder<T, R>().findCriticalPath(
                dag.getRootNodes(), dag.getLeafNodes(), nodeWeights);
    }

    private double calculateParallelizationPotential(DAG<T, R> dag, List<DAGNode<T, R>> criticalPath) {
        int totalNodes = dag.getNodes().size();
        int criticalPathLength = criticalPath.size();
        return (double) totalNodes / criticalPathLength;
    }
}
