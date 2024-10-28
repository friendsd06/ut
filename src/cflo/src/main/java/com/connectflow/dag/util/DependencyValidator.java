package com.connectflow.dag.util;

import com.connectflow.dag.DAGNode;

import java.util.Collection;

public class DependencyValidator<T, R> {

    public void validateDependencies(Collection<DAGNode<T, R>> nodes) {
        for (DAGNode<T, R> node : nodes) {
            for (DAGNode<T, R> dep : node.getIncomingNodes()) {
                if (!nodes.contains(dep)) {
                    throw new IllegalStateException("Dependency node not found in DAG: " + dep.getId());
                }
            }
        }
    }
}
