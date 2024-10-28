package com.connectflow.dag.listeners;

import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.execution.DAGExecutionResult;

public class NotificationListener<T, R> implements DAGExecutionListener<T, R>  {

    @Override
    public void onNodeStarted(DAGNode<T, R> node) {}

    @Override
    public void onNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result) {}

    @Override
    public void onNodeFailed(DAGNode<T, R> node, Exception error) {}

    @Override
    public void onDagCompleted(DAGExecutionResult<T, R> result) {}

    @Override
    public void updateVisualization(DAGNode<T, R> node) {}
}
