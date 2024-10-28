package com.connectflow.dag.listeners;

import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.execution.DAGExecutionResult;
import com.connectflow.dag.service.AuditService;

public class AuditTrailListener<T, R> implements DAGExecutionListener<T, R> {

    @Override
    public void onNodeStarted(DAGNode<T, R> node) {
        AuditService.recordEvent("TaskStarted", node.getId(), System.currentTimeMillis());
    }


    @Override
    public void onNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result) {
        AuditService.recordEvent("TaskCompleted", node.getId(), System.currentTimeMillis());
    }

    @Override
    public void onNodeFailed(DAGNode<T, R> node, Exception error) {
        AuditService.recordEvent("TaskFailed", node.getId(), System.currentTimeMillis());
    }

    @Override
    public void updateVisualization(DAGNode<T, R> node) {

    }

    @Override
    public void onDagCompleted(DAGExecutionResult<T, R> result) {
        AuditService.recordEvent("DagCompleted", "DAG", System.currentTimeMillis());
    }
}
