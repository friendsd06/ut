package com.connectflow.dag.listeners;

import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.execution.DAGExecutionResult;

public interface DAGExecutionListener<T, R> {

    void onNodeStarted(DAGNode<T, R> node);

    void onNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result);

    void onNodeFailed(DAGNode<T, R> node, Exception error);

    void onDagCompleted(DAGExecutionResult<T, R> result);

    void updateVisualization(DAGNode<T, R> node);
}
