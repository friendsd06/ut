package com.connectflow.dag.util;
import com.connectflow.dag.DAGNode;

import java.util.*;

public class CycleDetector<T, R> {

    public boolean wouldCreateCycle(DAGNode<T, R> fromNode, DAGNode<T, R> toNode) {
        Set<DAGNode<T, R>> visited = new HashSet<>();
        Deque<DAGNode<T, R>> stack = new ArrayDeque<>();
        stack.push(toNode);

        while (!stack.isEmpty()) {
            DAGNode<T, R> current = stack.pop();
            if (current.equals(fromNode)) {
                return true;
            }
            if (visited.add(current)) {
                stack.addAll(current.getOutgoingNodes());
            }
        }
        return false;
    }

    public Optional<List<DAGNode<T, R>>> detectCycles(Collection<DAGNode<T, R>> nodes) {
        Set<DAGNode<T, R>> visited = new HashSet<>();
        Set<DAGNode<T, R>> recStack = new HashSet<>();

        for (DAGNode<T, R> node : nodes) {
            if (detectCyclesUtil(node, visited, recStack)) {
                return Optional.of(new ArrayList<>(recStack));
            }
        }
        return Optional.empty();
    }

    private boolean detectCyclesUtil(DAGNode<T, R> node, Set<DAGNode<T, R>> visited, Set<DAGNode<T, R>> recStack) {
        if (recStack.contains(node)) {
            return true;
        }
        if (visited.contains(node)) {
            return false;
        }

        visited.add(node);
        recStack.add(node);

        for (DAGNode<T, R> neighbor : node.getOutgoingNodes()) {
            if (detectCyclesUtil(neighbor, visited, recStack)) {
                return true;
            }
        }

        recStack.remove(node);
        return false;
    }
}
