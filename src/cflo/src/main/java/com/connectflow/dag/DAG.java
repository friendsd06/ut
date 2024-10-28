package com.connectflow.dag;

import com.connectflow.dag.metrics.DAGStatistics;
import com.connectflow.dag.metrics.DAGStatisticsCalculator;
import com.connectflow.dag.util.CycleDetector;
import com.connectflow.dag.util.DependencyValidator;
import com.connectflow.dag.util.TopologicalSorter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DAG<T, R> {
    private final Map<T, DAGNode<T, R>> nodes;
    private final CycleDetector<T, R> cycleDetector;
    private final DependencyValidator<T, R> validator;

    public DAG() {
        this.nodes = new ConcurrentHashMap<>();
        this.cycleDetector = new CycleDetector<>();
        this.validator = new DependencyValidator<>();
    }

    public void addNode(DAGNode<T, R> node) {
        if (node == null) {
            throw new IllegalArgumentException("Node cannot be null");
        }
        nodes.put(node.getId(), node);
    }

    public void addDependency(T from, T to) {
        DAGNode<T, R> fromNode = nodes.get(from);
        DAGNode<T, R> toNode = nodes.get(to);

        if (fromNode == null || toNode == null) {
            throw new IllegalArgumentException("Both nodes must exist in the DAG");
        }

        if (cycleDetector.wouldCreateCycle(fromNode, toNode)) {
            throw new IllegalArgumentException("Adding this dependency would create a cycle");
        }

        fromNode.addOutgoingNode(toNode);
        toNode.addIncomingNode(fromNode);
    }

    public List<DAGNode<T, R>> getExecutionOrder() {
        validateDAG();
        return new TopologicalSorter<T, R>().sort(nodes.values());
    }

    public Set<DAGNode<T, R>> getRootNodes() {
        return nodes.values().stream()
                .filter(node -> node.getIncomingNodes().isEmpty())
                .collect(Collectors.toSet());
    }

    public Set<DAGNode<T, R>> getLeafNodes() {
        return nodes.values().stream()
                .filter(node -> node.getOutgoingNodes().isEmpty())
                .collect(Collectors.toSet());
    }

    public void validateDAG() {
        cycleDetector.detectCycles(nodes.values())
                .ifPresent(cycle -> {
                    throw new IllegalStateException("Cycle detected in DAG: " + cycle);
                });
        validator.validateDependencies(nodes.values());
    }

    public DAGStatistics<T, R> getStatistics() {
        return new DAGStatisticsCalculator<T, R>().calculateStatistics(this);
    }

    public Set<DAGNode<T, R>> getNodes() {
        return new HashSet<>(nodes.values());
    }
}
