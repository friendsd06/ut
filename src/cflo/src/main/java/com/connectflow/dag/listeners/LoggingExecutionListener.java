package com.connectflow.dag.listeners;


import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.execution.DAGExecutionResult;

public class LoggingExecutionListener<T, R> implements DAGExecutionListener<T, R> {

    @Override
    public void onNodeStarted(DAGNode<T, R> node) {
        System.out.println("Node started: {}"+node.getId());
    }
    @Override
    public void onNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result) {}
    @Override
    public void onNodeFailed(DAGNode<T, R> node, Exception e) {
        System.out.println("Node failed: {}"+node.getId());
    }
    @Override
    public void updateVisualization(DAGNode<T, R> node) {}

    @Override
    public void onDagCompleted(DAGExecutionResult<T, R> result) {
        System.out.println("DAG execution completed in {} ms"+result.getExecutionTime());
    }
}
