package com.connectflow.dag.util;

import com.connectflow.dag.DAGNode;

import java.util.*;

public class TopologicalSorter<T, R> {

    public List<DAGNode<T, R>> sort(Collection<DAGNode<T, R>> nodes) {
        List<DAGNode<T, R>> sortedList = new ArrayList<>();
        Set<DAGNode<T, R>> visited = new HashSet<>();

        for (DAGNode<T, R> node : nodes) {
            if (!visited.contains(node)) {
                sortUtil(node, visited, sortedList);
            }
        }
        Collections.reverse(sortedList);
        return sortedList;
    }

    private void sortUtil(DAGNode<T, R> node, Set<DAGNode<T, R>> visited, List<DAGNode<T, R>> sortedList) {
        visited.add(node);

        for (DAGNode<T, R> neighbor : node.getOutgoingNodes()) {
            if (!visited.contains(neighbor)) {
                sortUtil(neighbor, visited, sortedList);
            }
        }

        sortedList.add(node);
    }
}
