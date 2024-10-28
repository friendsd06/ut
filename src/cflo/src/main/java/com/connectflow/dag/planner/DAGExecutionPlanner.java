package com.connectflow.dag.planner;

import com.connectflow.dag.DAG;
import com.connectflow.dag.metrics.DAGStatistics;
import com.connectflow.dag.execution.FailureStrategy;

public class DAGExecutionPlanner<T, R> {

    public DAGExecutionPlan<T, R> createExecutionPlan(DAG<T, R> dag) {
        dag.validateDAG();
        FailureStrategy failureStrategy = determineFailureStrategy(dag);
        return new DAGExecutionPlan<>(dag, failureStrategy);
    }

    private FailureStrategy determineFailureStrategy(DAG<T, R> dag) {
        DAGStatistics<T, R> stats = dag.getStatistics();
        if (stats.getCriticalPath().size() == stats.getNodeCount()) {
            return FailureStrategy.FAIL_FAST;
        } else if (stats.getParallelizationPotential() > 2.0) {
            return FailureStrategy.CONTINUE;
        } else {
            return FailureStrategy.RETRY;
        }
    }
}
