package com.connectflow.dag;

import com.connectflow.dag.tasks.Task;
import com.connectflow.dag.tasks.TaskStatus;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DAGNode<T, R> {
    private final T id;
    private final Task<T, R> task;
    private final Set<DAGNode<T, R>> incomingNodes;
    private final Set<DAGNode<T, R>> outgoingNodes;
    private volatile TaskStatus status;
    private volatile ExecutionResult<T, R> result;
    private final Duration estimatedDuration;

    public DAGNode(T id, Task<T, R> task, Duration estimatedDuration) {
        if (id == null || task == null || estimatedDuration == null) {
            throw new IllegalArgumentException("id, task, and estimatedDuration must not be null");
        }
        this.id = id;
        this.task = task;
        this.incomingNodes = ConcurrentHashMap.newKeySet();
        this.outgoingNodes = ConcurrentHashMap.newKeySet();
        this.status = TaskStatus.PENDING;
        this.estimatedDuration = estimatedDuration;
    }

    public T getId() {
        return id;
    }

    public Task<T, R> getTask() {
        return task;
    }

    public Set<DAGNode<T, R>> getIncomingNodes() {
        return incomingNodes;
    }

    public Set<DAGNode<T, R>> getOutgoingNodes() {
        return outgoingNodes;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public ExecutionResult<T, R> getResult() {
        return result;
    }

    public void setResult(ExecutionResult<T, R> result) {
        this.result = result;
    }

    public Duration getEstimatedDuration() {
        return estimatedDuration;
    }

    public void addIncomingNode(DAGNode<T, R> node) {
        if (node == null) {
            throw new IllegalArgumentException("Incoming node cannot be null");
        }
        incomingNodes.add(node);
    }

    public void addOutgoingNode(DAGNode<T, R> node) {
        if (node == null) {
            throw new IllegalArgumentException("Outgoing node cannot be null");
        }
        outgoingNodes.add(node);
    }
}
