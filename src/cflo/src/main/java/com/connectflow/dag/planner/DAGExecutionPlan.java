package com.connectflow.dag.planner;

import com.connectflow.dag.DAG;
import com.connectflow.dag.DAGNode;
import com.connectflow.dag.tasks.TaskStatus;
import com.connectflow.dag.execution.FailureStrategy;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class DAGExecutionPlan<T, R> {
    private final DAG<T, R> dag;
    private final Set<DAGNode<T, R>> completedNodes;
    private final Lock planLock;
    private final FailureStrategy failureStrategy;
    private final long startTime;

    public DAGExecutionPlan(DAG<T, R> dag, FailureStrategy failureStrategy) {
        if (dag == null || failureStrategy == null) {
            throw new IllegalArgumentException("DAG and FailureStrategy cannot be null");
        }
        this.dag = dag;
        this.completedNodes = ConcurrentHashMap.newKeySet();
        this.planLock = new ReentrantLock();
        this.failureStrategy = failureStrategy;
        this.startTime = System.currentTimeMillis();
    }

    public Set<DAGNode<T, R>> getInitialNodes() {
        return dag.getRootNodes();
    }

    public Set<DAGNode<T, R>> getAllNodes() {
        return dag.getNodes();
    }

    public boolean isExecutionComplete() {
        planLock.lock();
        try {
            return completedNodes.size() == dag.getNodes().size();
        } finally {
            planLock.unlock();
        }
    }

    public Set<DAGNode<T, R>> getReadyNodes() {
        planLock.lock();
        try {
            return dag.getNodes().stream()
                    .filter(node -> node.getStatus() == TaskStatus.PENDING)
                    .filter(node -> node.getIncomingNodes().stream()
                            .allMatch(parent -> parent.getStatus() == TaskStatus.COMPLETED))
                    .collect(Collectors.toSet());
        } finally {
            planLock.unlock();
        }
    }

    public void markNodeCompleted(DAGNode<T, R> node) {
        planLock.lock();
        try {
            completedNodes.add(node);
        } finally {
            planLock.unlock();
        }
    }

    public FailureStrategy getFailureStrategy() {
        return failureStrategy;
    }

    public long getStartTime() {
        return startTime;
    }
}
