package com.connectflow.dag.util;

import com.connectflow.dag.DAGNode;

import java.util.*;

public class ExecutionPathFinder<T, R> {

    public List<DAGNode<T, R>> findCriticalPath(Set<DAGNode<T, R>> rootNodes,
                                                Set<DAGNode<T, R>> leafNodes,
                                                Map<DAGNode<T, R>, Long> nodeWeights) {
        Map<DAGNode<T, R>, Long> distances = new HashMap<>();
        Map<DAGNode<T, R>, DAGNode<T, R>> predecessors = new HashMap<>();

        for (DAGNode<T, R> node : nodeWeights.keySet()) {
            distances.put(node, Long.MIN_VALUE);
        }

        for (DAGNode<T, R> root : rootNodes) {
            distances.put(root, nodeWeights.get(root));
            dfs(root, nodeWeights, distances, predecessors);
        }

        DAGNode<T, R> endNode = null;
        long maxDistance = Long.MIN_VALUE;
        for (DAGNode<T, R> leaf : leafNodes) {
            if (distances.get(leaf) > maxDistance) {
                maxDistance = distances.get(leaf);
                endNode = leaf;
            }
        }

        // Reconstruct path
        List<DAGNode<T, R>> criticalPath = new ArrayList<>();
        DAGNode<T, R> currentNode = endNode;
        while (currentNode != null) {
            criticalPath.add(currentNode);
            currentNode = predecessors.get(currentNode);
        }
        Collections.reverse(criticalPath);

        return criticalPath;
    }

    private void dfs(DAGNode<T, R> node, Map<DAGNode<T, R>, Long> nodeWeights,
                     Map<DAGNode<T, R>, Long> distances, Map<DAGNode<T, R>, DAGNode<T, R>> predecessors) {
        for (DAGNode<T, R> neighbor : node.getOutgoingNodes()) {
            long newDistance = distances.get(node) + nodeWeights.get(neighbor);
            if (newDistance > distances.get(neighbor)) {
                distances.put(neighbor, newDistance);
                predecessors.put(neighbor, node);
                dfs(neighbor, nodeWeights, distances, predecessors);
            }
        }
    }
}
